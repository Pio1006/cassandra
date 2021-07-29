/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.base.Charsets;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import com.carrotsearch.hppc.IntLongHashMap;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.packed.DirectWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.cassandra.index.sai.disk.v1.BlockIndexWriter.LEAF_SIZE;

public class BlockIndexReader
{
    final FileHandle indexFile;
    final PackedLongValues leafFilePointers;
    final IntLongHashMap nodeIDToLeaf = new IntLongHashMap();
    final IntLongHashMap leafToOrderMapFP = new IntLongHashMap();
    final SeekingRandomAccessInput seekingInput;
    final IndexInput input;
    final BytesRefBuilder builder = new BytesRefBuilder();
    final BlockIndexWriter.BlockIndexMeta meta;

    final IntLongHashMap nodeIDToPostingsFP;
    final IndexInput orderMapInput, postingsInput;
    final SeekingRandomAccessInput orderMapRandoInput;
    private final DirectReaders.Reader orderMapReader;

    DirectReaders.Reader lengthsReader, prefixesReader;
    int lengthsBytesLen;
    int prefixBytesLen;
    byte lengthsBits;
    byte prefixBits;
    long arraysFilePointer;

    int leafSize;

    private long leafBytesFP; // current file pointer in the bytes part of the leaf
    private long leafBytesStartFP; // file pointer where the overall bytes start for a leaf
    int bytesLength = 0;
    int lastLen = 0;
    int lastPrefix = 0;
    byte[] firstTerm;

    int leaf;
    int leafIndex;
    long currentLeafFP = -1;

    final RangeSet<Integer> multiBlockLeafOrdinalRanges;

    public BlockIndexReader(final IndexInput input,
                            final IndexInput input2,
                            final FileHandle indexFile,
                            final IndexInput orderMapInput,
                            final IndexInput postingsInput,
                            BlockIndexWriter.BlockIndexMeta meta) throws IOException
    {
        this.input = input;
        this.meta = meta;
        this.orderMapInput = orderMapInput;
        orderMapRandoInput = new SeekingRandomAccessInput(orderMapInput);
        this.multiBlockLeafOrdinalRanges = meta.multiBlockLeafOrdinalRanges;

        this.nodeIDToPostingsFP = meta.nodeIDPostingsFP;
        this.postingsInput = postingsInput;

        orderMapReader = DirectReaders.getReaderForBitsPerValue((byte) DirectWriter.unsignedBitsRequired(LEAF_SIZE - 1));

        seekingInput = new SeekingRandomAccessInput(input2);

        final PackedLongValues.Builder leafFPBuilder = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);

        input.seek(meta.leafFilePointersFP);
        for (int x = 0; x < meta.numLeaves; x++)
        {
            long leafFP = input.readVLong();
            System.out.println("leafFP="+leafFP);
            leafFPBuilder.add(leafFP);
        }
        leafFilePointers = leafFPBuilder.build();

        final PackedLongValues.Builder nodeIDToLeafOrdinalFPBuilder = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
        nodeIDToLeafOrdinalFPBuilder.add(0);

        input.seek(meta.nodeIDToLeafOrdinalFP);
        final int numNodes = input.readVInt();
        for (int x = 1; x <= numNodes; x++)
        {
            int nodeID = input.readVInt(); // TODO: en/decoding the node ID isn't necessary since it's in order
            int leafOrdinal = input.readVInt();

            assert nodeID == x;

            nodeIDToLeaf.put(nodeID, leafOrdinal);
        }

        orderMapInput.seek(meta.orderMapFP);
        final int numOrderMaps = orderMapInput.readVInt();
        for (int x = 0; x < numOrderMaps; x++)
        {
            int leaf = orderMapInput.readVInt(); // TODO: en/decoding the node ID isn't necessary since it's in order
            long fp = orderMapInput.readVLong();
            leafToOrderMapFP.put(leaf, fp);
        }

        this.indexFile = indexFile;
    }

    public SortedSet<Integer> traverseForNodeIDs(ByteComparable start,
                                                 ByteComparable end) throws IOException
    {
        Pair<Integer, Integer> pair = traverseForMinMaxLeafOrdinals(start, end);
        int min = pair.left;
        int max = pair.right;
        if (pair.right == -1)
        {
            max = meta.numLeaves;
        }
        if (pair.left == -1)
        {
            min = 0;
        }
        Range<Integer> multiBlockRange = multiBlockLeafOrdinalRanges.rangeContaining(max);
        if (multiBlockRange != null)
        {
            max = multiBlockRange.upperEndpoint();
        }
        System.out.println("multiBlockRange="+multiBlockRange+" max="+max+" multiBlockLeafOrdinalRanges="+multiBlockLeafOrdinalRanges);
        TreeSet<Integer> nodeIDs = traverseIndex(min, max);
        System.out.println("traverseForNodeIDs min/max="+pair+" nodeIDs="+nodeIDs+" min="+min+" max="+max);
        return nodeIDs;

//        TreeSet<Long> filePointers = new TreeSet<>();
//
//        for (int nodeID : nodeIDs)
//        {
//            int leafOrdinal = (int) nodeIDToLeaf.get(nodeID);
//            long filePointer = nodeIDToFilePointer.get(leafOrdinal);
//
//            filePointers.add(filePointer);
//        }

        // return filePointers;
    }

    public PostingList filterLastLeaf(int nodeID,
                                      BytesRef targetTerm,
                                      boolean exclusive) throws IOException
    {
        final int leaf = (int) this.nodeIDToLeaf.get(nodeID);
        final long leafFP = this.leafFilePointers.get(leaf);
        readBlock(leafFP);

        int idx = 0;
        int endIdx = this.leafSize - 1;
        for (idx = 0; idx < this.leafSize; idx++)
        {
            final BytesRef term = seekInBlock(idx);

            if (term.compareTo(targetTerm) > 0)
            {
                endIdx = idx - 1;
                break;
            }
        }

        //int cardinality = this.leafSize - idx;
        int cardinality = idx;
        final int endIdxFinal = endIdx;

        final long orderMapFP = leafToOrderMapFP.get(leaf);
        final long postingsFP = nodeIDToPostingsFP.get(nodeID);
        System.out.println("nodeID=" + nodeID + " postingsFP=" + postingsFP);
        PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(postingsInput, postingsFP);
        PostingsReader postings = new PostingsReader(postingsInput, summary, QueryEventListener.PostingListEventListener.NO_OP);
        FilteringPostingList2 filterPostings = new FilteringPostingList2(
        cardinality,
        // get the row id's term ordinal to compare against the end ord
        (postingsOrd, rowID) -> this.orderMapReader.get(this.orderMapRandoInput, orderMapFP, postingsOrd) <= endIdxFinal,
        postings);
        return filterPostings;
    }

    public PostingList filterFirstLeaf(int nodeID,
                                       BytesRef targetTerm,
                                       boolean exclusive) throws IOException
    {
        final int leaf = (int) this.nodeIDToLeaf.get(nodeID);
        final long leafFP = leafFilePointers.get(leaf);
        readBlock(leafFP);

        int idx = 0;
        for (idx = 0; idx < this.leafSize; idx++)
        {
            final BytesRef term = seekInBlock(idx);

            if (exclusive)
            {
                if (term.compareTo(targetTerm) > 0)
                {
                    break;
                }
            }
            else
            {
                if (term.compareTo(targetTerm) >= 0)
                {
                    break;
                }
            }
        }

        int cardinality = this.leafSize - idx;

        final int startOrdinal = idx;

        final long orderMapFP = leafToOrderMapFP.get(leaf);
        final long postingsFP = nodeIDToPostingsFP.get(nodeID);
        System.out.println("nodeID="+nodeID+" postingsFP="+postingsFP);
        PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(postingsInput, postingsFP);
        PostingsReader postings = new PostingsReader(postingsInput, summary, QueryEventListener.PostingListEventListener.NO_OP);
        FilteringPostingList2 filterPostings = new FilteringPostingList2(
        cardinality,
        // get the row id's term ordinal to compare against the startOrdinal
        (postingsOrd, rowID) -> this.orderMapReader.get(this.orderMapRandoInput, orderMapFP, postingsOrd) >= startOrdinal,
        postings);
        return filterPostings;
    }

    public BinaryTreeIndex binaryTreeIndex()
    {
        return new BinaryTreeIndex(meta.numLeaves);
    }

    // returns node id's
    public TreeSet<Integer> traverseIndex(int minLeaf, int maxLeaf) throws IOException
    {
        SimpleRangeVisitor visitor = new SimpleRangeVisitor(new BKDReader.SimpleBound(minLeaf, true),
                                                            new BKDReader.SimpleBound(maxLeaf, false));
        TreeSet<Integer> resultNodeIDs = new TreeSet();

        BinaryTreeIndex index = new BinaryTreeIndex(meta.numLeaves);

        collectPostingLists(0,
                            (int) nodeIDToLeaf.size() - 1,
                            index,
                            visitor,
                            resultNodeIDs);

        System.out.println("traverseIndex resultNodeIDs=" + resultNodeIDs);

        return resultNodeIDs;
    }

    protected void collectPostingLists(int cellMinLeafOrdinal,
                                       int cellMaxLeafOrdinal,
                                       BinaryTreeIndex index,
                                       SimpleVisitor visitor,
                                       Set<Integer> resultNodeIDs) throws IOException
    {
        final PointValues.Relation r = visitor.compare(cellMinLeafOrdinal, cellMaxLeafOrdinal);

        if (r == PointValues.Relation.CELL_OUTSIDE_QUERY)
        {
            // This cell is fully outside of the query shape: stop recursing
            return;
        }

        if (r == PointValues.Relation.CELL_INSIDE_QUERY)
        {
            final int nodeID = index.getNodeID();

            // if there is pre-built posting for entire subtree
            if (nodeIDToPostingsFP.containsKey(nodeID))
            {
                resultNodeIDs.add(nodeID);
                return;
            }

            // TODO: assert that the node is part of a multi-block postings
            //Preconditions.checkState(!index.isLeafNode(), "Leaf node %s does not have kd-tree postings.", index.getNodeID());

            visitNode(cellMinLeafOrdinal,
                      cellMaxLeafOrdinal,
                      index,
                      visitor,
                      resultNodeIDs);
            return;
        }

        if (index.isLeafNode())
        {
            if (index.nodeExists())
            {
                int nodeID = index.getNodeID();
                System.out.println("leafNodeID="+nodeID);
                resultNodeIDs.add(nodeID);
            }
            return;
        }

        visitNode(cellMinLeafOrdinal,
                  cellMaxLeafOrdinal,
                  index,
                  visitor,
                  resultNodeIDs);
    }

    void visitNode(int cellMinPacked,
                   int cellMaxPacked,
                   BinaryTreeIndex index,
                   SimpleVisitor visitor,
                   Set<Integer> resultNodeIDs) throws IOException
    {
        int nodeID = index.getNodeID();
        int splitLeafOrdinal = (int) nodeIDToLeaf.get(nodeID);

        index.pushLeft();
        collectPostingLists(cellMinPacked, splitLeafOrdinal, index, visitor, resultNodeIDs);
        index.pop();

        index.pushRight();
        collectPostingLists(splitLeafOrdinal, cellMaxPacked, index, visitor, resultNodeIDs);
        index.pop();
    }

    public Pair<Integer,Integer> traverseForMinMaxLeafOrdinals(ByteComparable start, ByteComparable end) throws IOException
    {
        int minLeafOrdinal = -1, maxLeafOrdinal = -1;

        if (start != null)
        {
            try (TermsRangeIterator reader = new TermsRangeIterator(indexFile.instantiateRebufferer(),
                                                                    meta.indexFP,
                                                                    start,
                                                                    null,
                                                                    true,
                                                                    true))
            {
                Iterator<Pair<ByteSource, Long>> iterator = reader.iterator();
                Pair<ByteSource, Long> pair = iterator.next();
                minLeafOrdinal = pair.right.intValue();
            }
        }

        if (end != null)
        {
            try (TermsRangeIterator reader = new TermsRangeIterator(indexFile.instantiateRebufferer(),
                                                                    meta.indexFP,
                                                                    end,
                                                                    null,
                                                                    true,
                                                                    true))
            {
                Iterator<Pair<ByteSource, Long>> iterator = reader.iterator();
                Pair<ByteSource, Long> pair = iterator.next();
                maxLeafOrdinal = pair.right.intValue();
            }
        }

        System.out.println("minLeafOrdinal="+minLeafOrdinal+" maxLeafOrdinal="+maxLeafOrdinal);

        return Pair.create(minLeafOrdinal, maxLeafOrdinal);
    }

    public BytesRef seekTo(long pointID) throws IOException
    {
        final long leaf = pointID / LEAF_SIZE;
        final int leafIdx = (int) (pointID % LEAF_SIZE);

        final long leafFP = leafFilePointers.get(leaf);

        System.out.println("leaf="+leaf+" pointID="+pointID+" leafIdx="+leafIdx+" leafFP="+leafFP);

        if (currentLeafFP != leafFP)
        {
            readBlock(leafFP);
            this.currentLeafFP = leafFP;
        }
        return seekInBlock(leafIdx);
    }

    public static ByteComparable fixedLength(BytesRef bytes)
    {
        return ByteComparable.fixedLength(bytes.bytes, bytes.offset, bytes.length);
    }

    public BytesRef seekTo(BytesRef target) throws IOException
    {
        try (TermsRangeIterator reader = new TermsRangeIterator(indexFile.instantiateRebufferer(),
                                                                meta.indexFP,
                                                                fixedLength(target),
                                                                null,
                                                                false,
                                                                true))
        {
            Iterator<Pair<ByteSource, Long>> iterator = reader.iterator();
            Pair<ByteSource, Long> pair = iterator.next();
            int leafOrdinal = pair.right.intValue();

            System.out.println("leafOrdinal=" + pair.right + " term=" + new String(ByteSourceInverse.readBytes(pair.left, 10), Charsets.UTF_8));

            if (leafOrdinal == this.leaf)
            {

            }
            else
            {
                final long leafFP = leafFilePointers.get(leafOrdinal);
                this.leaf = leafOrdinal;
                readBlock(leafFP);
            }

            for (int x = 0; x < leafSize; x++)
            {
                BytesRef term = seekInBlock(x);
                System.out.println("seekInBlock term="+term.utf8ToString());
                if (target.compareTo(term) <= 0)
                {
                    return term;
                }
            }

            return null;
        }
    }

    private void readBlock(long filePointer) throws IOException
    {
        System.out.println("readBlock filePointer="+filePointer);
        input.seek(filePointer);
        this.currentLeafFP = filePointer;
        leafSize = input.readInt();
        lengthsBytesLen = input.readInt();
        prefixBytesLen = input.readInt();
        lengthsBits = input.readByte();
        prefixBits = input.readByte();

        arraysFilePointer = input.getFilePointer();

        System.out.println("arraysFilePointer="+arraysFilePointer+" lengthsBytesLen="+lengthsBytesLen+" prefixBytesLen="+prefixBytesLen+" lengthsBits="+lengthsBits+" prefixBits="+prefixBits);

        lengthsReader = DirectReaders.getReaderForBitsPerValue(lengthsBits);
        prefixesReader = DirectReaders.getReaderForBitsPerValue(prefixBits);

        input.seek(arraysFilePointer + lengthsBytesLen + prefixBytesLen);

        leafBytesStartFP = leafBytesFP = input.getFilePointer();

        this.leafIndex = 0;
    }

    public BytesRef seekInBlock(int seekIndex) throws IOException
    {
//        if (seekIndex < index)
//        {
//            throw new IllegalArgumentException("seekIndex="+seekIndex+" must be greater than index="+index);
//        }
        if (seekIndex >= leafSize)
        {
            throw new IllegalArgumentException("seekIndex="+seekIndex+" must be less than the leaf size="+leafSize);
        }

        System.out.println("seekInBlock seekInBlock="+seekIndex+" leafIndex="+leafIndex);

        int len = 0;
        int prefix = 0;

        // TODO: this part can go back from the current
        //       position rather than start from the beginning each time

        int start = 0;

        if (seekIndex >= leafIndex)
        {
            start = leafIndex;
        }

        for (int x = start; x <= seekIndex; x++)
        {
            len = LeafOrderMap.getValue(seekingInput, arraysFilePointer, x, lengthsReader);
            prefix = LeafOrderMap.getValue(seekingInput, arraysFilePointer + lengthsBytesLen, x, prefixesReader);

            System.out.println("x="+x+" len="+len+" prefix="+prefix);

            if (x == 0)
            {
                firstTerm = new byte[len];
                input.seek(leafBytesStartFP);
                input.readBytes(firstTerm, 0, len);
                lastPrefix = len;
                System.out.println("firstTerm="+new BytesRef(firstTerm).utf8ToString());
                bytesLength = 0;
                leafBytesFP += len;
            }

            if (len > 0 && x > 0)
            {
                bytesLength = len - prefix;
                lastLen = len;
                lastPrefix = prefix;
                System.out.println("x=" + x + " bytesLength=" + bytesLength + " len=" + len + " prefix=" + prefix);
            }
            else
            {
                bytesLength = 0;
            }
        }

        this.leafIndex = seekIndex + 1;

        if (!(len == 0 && prefix == 0))
        {
            builder.clear();

            System.out.println("bytesPosition=" + leafBytesFP + " bytesPositionStart=" + leafBytesStartFP + " total=" + (leafBytesFP - leafBytesStartFP));

            System.out.println("lastlen=" + lastLen + " lastPrefix=" + lastPrefix + " bytesLength=" + bytesLength);
            final byte[] bytes = new byte[bytesLength];
            input.seek(leafBytesFP);
            input.readBytes(bytes, 0, bytesLength);

            leafBytesFP += bytesLength;

            System.out.println("bytes read=" + new BytesRef(bytes).utf8ToString());

            builder.append(firstTerm, 0, lastPrefix);
            builder.append(bytes, 0, bytes.length);
        }

        System.out.println("str="+builder.get().utf8ToString());
        return builder.get();
    }

    interface SimpleVisitor
    {
        PointValues.Relation compare(int minOrdinal, int maxOrdinal);
    }

    public static class SimpleBound
    {
        private final int bound;
        private final boolean exclusive;

        public SimpleBound(int bound, boolean exclusive)
        {
            this.bound = bound;
            this.exclusive = exclusive;
        }

        public boolean smallerThan(int cmp)
        {
            return cmp > 0 || (cmp == 0 && exclusive);
        }

        public boolean greaterThan(int cmp)
        {
            return cmp < 0 || (cmp == 0 && exclusive);
        }
    }

    static class SimpleRangeVisitor implements SimpleVisitor
    {
        final BKDReader.SimpleBound lower, upper;

        public SimpleRangeVisitor(BKDReader.SimpleBound lower, BKDReader.SimpleBound upper)
        {
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public PointValues.Relation compare(int minValue, int maxValue)
        {
            boolean crosses = false;

            if (lower != null)
            {
                int maxCmp = Integer.compare(maxValue, lower.bound);
                if (lower.greaterThan(maxCmp))
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;

                int minCmp = Integer.compare(minValue, lower.bound);
                crosses |= lower.greaterThan(minCmp);
            }

            if (upper != null)
            {
                int minCmp = Integer.compare(minValue, upper.bound);
                if (upper.smallerThan(minCmp))
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;

                int maxCmp = Integer.compare(maxValue, upper.bound);
                crosses |= upper.smallerThan(maxCmp);
            }

            if (crosses)
            {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
            else
            {
                return PointValues.Relation.CELL_INSIDE_QUERY;
            }
        }
    }
}
