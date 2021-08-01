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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.base.Charsets;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

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
import org.apache.lucene.util.NumericUtils;
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
    final IndexInput multiPostingsInput;

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

    final RangeSet<Integer> multiBlockLeafRanges;

    public BlockIndexReader(final IndexInput input,
                            final IndexInput input2,
                            final FileHandle indexFile,
                            final IndexInput orderMapInput,
                            final IndexInput postingsInput,
                            final IndexInput multiPostingsInput,
                            BlockIndexWriter.BlockIndexMeta meta) throws IOException
    {
        this.input = input;
        this.meta = meta;
        this.orderMapInput = orderMapInput;
        orderMapRandoInput = new SeekingRandomAccessInput(orderMapInput);
        this.multiBlockLeafRanges = meta.multiBlockLeafOrdinalRanges;
        this.multiPostingsInput = multiPostingsInput;

        this.nodeIDToPostingsFP = meta.nodeIDPostingsFP;
        this.postingsInput = postingsInput;

        orderMapReader = DirectReaders.getReaderForBitsPerValue((byte) DirectWriter.unsignedBitsRequired(LEAF_SIZE - 1));

        seekingInput = new SeekingRandomAccessInput(input2);

        final PackedLongValues.Builder leafFPBuilder = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);

        input.seek(meta.leafFilePointersFP);
        for (int x = 0; x < meta.numLeaves; x++)
        {
            long leafFP = input.readVLong();
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

            assert nodeID == x : "nodeid="+nodeID+" x="+x;

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

    static class NodeIDLeafFP
    {
        public final int nodeID;
        public final int leaf;
        public final long filePointer;

        public NodeIDLeafFP(int nodeID, int leaf, long filePointer)
        {
            this.nodeID = nodeID;
            this.leaf = leaf;
            this.filePointer = filePointer;
        }

        @Override
        public String toString()
        {
            return "NodeIDLeafFP{" +
                   "nodeID=" + nodeID +
                   ", leaf=" + leaf +
                   ", filePointer=" + filePointer +
                   '}';
        }
    }

    public PostingList traverse(ByteComparable start,
                                ByteComparable end) throws IOException
    {
        SortedSet<Integer> nodeIDs = traverseForNodeIDs(start, end);

        // if there's only 1 leaf then filter on it
        if (nodeIDs.size() == 0 && meta.numLeaves == 1)
        {
            nodeIDs.add(this.nodeIDToLeaf.keys().iterator().next().value);
        }

        BytesRef startBytes = new BytesRef(ByteSourceInverse.readBytes(start.asComparableBytes(ByteComparable.Version.OSS41)));
        BytesRef endBytes = new BytesRef(ByteSourceInverse.readBytes(end.asComparableBytes(ByteComparable.Version.OSS41)));

        //List<Pair<Integer, Integer>> leafNodeIDToLeafOrd = new ArrayList<>();
        List<NodeIDLeafFP> leafNodeIDToLeafOrd = new ArrayList<>();

        for (int nodeID : nodeIDs)
        {
            final NavigableSet<Long> multiPostingFPs = this.meta.multiNodeIDToPostingsFP.get(nodeID);
            if (multiPostingFPs != null && multiPostingFPs.size() > 0)
            {
                final int leaf = (int) this.nodeIDToLeaf.get(nodeID);
                for (final long fp : multiPostingFPs)
                {
                    leafNodeIDToLeafOrd.add(new NodeIDLeafFP(nodeID, leaf, fp));
                }
            }
            else
            {
                int leafOrdinal = (int)nodeIDToLeaf.get(nodeID);
                Long postingsFP = null;
                if (meta.nodeIDPostingsFP.containsKey(nodeID))
                {
                    postingsFP = meta.nodeIDPostingsFP.get(nodeID);
                }

                if (postingsFP != null)
                {
                    leafNodeIDToLeafOrd.add(new NodeIDLeafFP(nodeID, leafOrdinal, postingsFP));
                }

                System.out.println("nodeID=" + nodeID + " leafOrdinal=" + leafOrdinal + " postingsFP=" + postingsFP);
            }
        }
        // sort by leaf id
        Collections.sort(leafNodeIDToLeafOrd, (o1, o2) -> Integer.compare(o1.leaf, o2.leaf));
        int minNodeID = leafNodeIDToLeafOrd.get(0).nodeID;
        int minLeafOrd = leafNodeIDToLeafOrd.get(0).leaf;
        int maxNodeID = leafNodeIDToLeafOrd.get(leafNodeIDToLeafOrd.size() - 1).nodeID;
        int maxLeafOrd = leafNodeIDToLeafOrd.get(leafNodeIDToLeafOrd.size() - 1).leaf;

        System.out.println("nodeIDToLeafOrd="+leafNodeIDToLeafOrd);

        final List<PostingList.PeekablePostingList> postingLists = new ArrayList<>();

        final boolean minRangeExists = meta.multiBlockLeafOrdinalRanges.contains(minLeafOrd);

        int startOrd = 1;
        int endOrd = leafNodeIDToLeafOrd.size() - 1;

        if (minLeafOrd == maxLeafOrd)
        {
            // TODO: if the minNode is all same values or multi-block there's
            //       no need to filter
            return filterLeaf(minNodeID,
                              startBytes,
                              endBytes,
                              false);
        }

        System.out.println("minLeafOrd=" + minLeafOrd + " maxLeafOrd=" + maxLeafOrd + " minRangeExists=" + minRangeExists);

        Integer firstFilterNodeID = null;

        if (minRangeExists)
        {
            startOrd = 0;
        }
        else
        {
            firstFilterNodeID = minNodeID;
            PostingList firstList = filterLeaf(minNodeID,
                                               startBytes,
                                               endBytes,
                                               false);//(minNodeID, startBytes, false);
            if (firstList != null)
            {
                postingLists.add(firstList.peekable());
            }
        }

        boolean maxRangeExists = meta.multiBlockLeafOrdinalRanges.contains(maxLeafOrd);
        boolean allSameValues = meta.leafValuesSame.get(maxLeafOrd);

        System.out.println("last leaf maxRangeExists="+maxRangeExists+" allSameValues="+allSameValues);

        if (maxRangeExists || allSameValues)
        {
            endOrd = leafNodeIDToLeafOrd.size();
            NodeIDLeafFP pair = leafNodeIDToLeafOrd.get(endOrd - 1);
            assert !leafToOrderMapFP.containsKey(pair.leaf);
        }
        else
        {
            if (firstFilterNodeID == null ||
                (firstFilterNodeID != null && firstFilterNodeID.intValue() != maxNodeID))
            {
                System.out.println("filterLastLeaf endBytes=" + NumericUtils.sortableBytesToInt(endBytes.bytes, 0));//endBytes.utf8ToString());
                PostingList lastList = filterLeaf(maxNodeID,
                                                  startBytes,
                                                  endBytes,
                                                  false);//filterLastLeaf(maxNodeID, endBytes, false);
                if (lastList != null)
                {
                    postingLists.add(lastList.peekable());
                }
            }
        }

        // make sure to iterate over the posting lists in leaf id order
        // TODO: the postings are not in leaf id order
        for (int x = startOrd; x < endOrd; x++)
        {
            NodeIDLeafFP nodeIDLeafOrd = leafNodeIDToLeafOrd.get(x);

            // negative file pointer means an upper level big posting list in multiPostingsInput
            if (nodeIDLeafOrd.filePointer < 0)
            {
                long fp = nodeIDLeafOrd.filePointer * -1;
                PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(multiPostingsInput, fp);
                PostingsReader postings = new PostingsReader(multiPostingsInput, summary, QueryEventListener.PostingListEventListener.NO_OP);
                postingLists.add(postings.peekable());
            }
            else
            {
                final long postingsFP = nodeIDToPostingsFP.get(nodeIDLeafOrd.nodeID);
                System.out.println("nodeID=" + nodeIDLeafOrd.nodeID + " postingsFP=" + postingsFP);
                PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(postingsInput, postingsFP);
                PostingsReader postings = new PostingsReader(postingsInput, summary, QueryEventListener.PostingListEventListener.NO_OP);
                postingLists.add(postings.peekable());
            }
        }
        PriorityQueue postingsQueue = new PriorityQueue(postingLists.size(), Comparator.comparingLong(PostingList.PeekablePostingList::peek));
        postingsQueue.addAll(postingLists);
        return MergePostingList.merge(postingsQueue);
    }

    public SortedSet<Integer> traverseForNodeIDs(ByteComparable start,
                                                 ByteComparable end) throws IOException
    {
        Pair<Integer, Integer> pair = traverseForMinMaxLeafOrdinals(start, end);
        int min = pair.left;
        int max = pair.right;
        System.out.println("traverseForNodeIDs pair="+pair);
        if (pair.right == -1)
        {
            max = meta.numLeaves;
        }
        if (pair.left == -1)
        {
            min = 0;
        }
        Range<Integer> multiBlockRange = multiBlockLeafRanges.rangeContaining(max);
        if (multiBlockRange != null)
        {
            max = multiBlockRange.upperEndpoint();
        }

        if (min > 0)
        {
            int prevMin = min - 1;
            boolean prevSameValues = meta.leafValuesSame.get(prevMin);
            System.out.println("     prevMin="+prevMin+" leafValuesSame="+prevSameValues);
            if (!prevSameValues)
            {
                System.out.println("   min-- min="+min);
                min--;
            }
        }

        System.out.println("multiBlockRange=" + multiBlockRange + " max=" + max + " multiBlockLeafRanges=" + multiBlockLeafRanges);

        TreeSet<Integer> nodeIDs = traverseIndex(min, max);
        System.out.println("traverseForNodeIDs min/max="+pair+" nodeIDs="+nodeIDs+" min="+min+" max="+max);
        return nodeIDs;
    }

    public PostingList filterLeaf(int nodeID,
                                  BytesRef start,
                                  BytesRef end,
                                  boolean exclusive) throws IOException
    {
        final int leaf = (int) this.nodeIDToLeaf.get(nodeID);

        // TODO: check if the leaf is all the same value
        //       if true, there's no need to filter

        final long leafFP = leafFilePointers.get(leaf);
        readBlock(leafFP);

        int idx = 0;
        int startIdx = -1;

        int endIdx = this.leafSize;

        for (idx = 0; idx < this.leafSize; idx++)
        {
            final BytesRef term = seekInBlock(idx);

//            System.out.println("filterFirstLastLeaf idx="+idx+" term=" + NumericUtils.sortableBytesToInt(term.bytes, 0)
//                               + " start=" + NumericUtils.sortableBytesToInt(start.bytes, 0)
//                               + " end=" + NumericUtils.sortableBytesToInt(end.bytes, 0));

            if (startIdx == -1 && term.compareTo(start) >= 0)
            {
                startIdx = idx;
            }

            if (term.compareTo(end) > 0)
            {
                endIdx = idx - 1;
                break;
            }
        }

        int cardinality = this.leafSize - startIdx;

        if (cardinality <= 0) return null;

        final Long orderMapFP;
        if (leafToOrderMapFP.containsKey(leaf))
        {
            orderMapFP = leafToOrderMapFP.get(leaf);
        }
        else
        {
            orderMapFP = null;
        }

        if (startIdx == -1) startIdx = this.leafSize;

        final int startIdxFinal = startIdx;
        final int endIdxFinal = endIdx;

        System.out.println("startIdxFinal="+startIdxFinal+" endIdxFinal="+endIdxFinal);

        final long postingsFP = nodeIDToPostingsFP.get(nodeID);
        System.out.println("leaf="+leaf+" nodeID=" + nodeID + " postingsFP=" + postingsFP + " startIdx=" + startIdx+" orderMapFP="+orderMapFP);
        PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(postingsInput, postingsFP);
        PostingsReader postings = new PostingsReader(postingsInput, summary, QueryEventListener.PostingListEventListener.NO_OP);
        FilteringPostingList2 filterPostings = new FilteringPostingList2(
        cardinality,
        // get the row id's term ordinal to compare against the startOrdinal
        (postingsOrd, rowID) -> {
            int ord = postingsOrd;
            if (orderMapFP != null)
            {
                ord = (int) this.orderMapReader.get(this.orderMapRandoInput, orderMapFP, postingsOrd);
            }
            System.out.println("postingsOrd="+postingsOrd+" ord="+ord+" startIdxFinal="+startIdxFinal+" endIdxFinal="+endIdxFinal+" rowID="+rowID);
            return ord >= startIdxFinal && ord <= endIdxFinal;
        },
        postings);
        return filterPostings;
    }

    public BinaryTreeIndex binaryTreeIndex()
    {
        return new BinaryTreeIndex(meta.numLeaves);
    }

    // using the given min and max leaf id's, traverse the binary tree, return node id's with postings
    // atm the that's only leaf node id's
    public TreeSet<Integer> traverseIndex(int minLeaf, int maxLeaf) throws IOException
    {
        SimpleRangeVisitor visitor = new SimpleRangeVisitor(new BKDReader.SimpleBound(minLeaf, true),
                                                            new BKDReader.SimpleBound(maxLeaf, false));
        TreeSet<Integer> resultNodeIDs = new TreeSet();

        BinaryTreeIndex index = new BinaryTreeIndex(meta.numLeaves);

        collectPostingLists(0,
                             nodeIDToLeaf.size() - 1,
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
        final int nodeID = index.getNodeID();
        final PointValues.Relation r = visitor.compare(cellMinLeafOrdinal, cellMaxLeafOrdinal);

        int leafID = (int)this.nodeIDToLeaf.get(nodeID);
        System.out.println("  collectPostingLists nodeID="+nodeID+" leafID="+leafID+" relation="+r);

        if (r == PointValues.Relation.CELL_OUTSIDE_QUERY)
        {
            // This cell is fully outside of the query shape: stop recursing
            return;
        }

        if (r == PointValues.Relation.CELL_INSIDE_QUERY)
        {
            // if there is pre-built posting list for the entire subtree
            if (nodeIDToPostingsFP.containsKey(nodeID))
            {
                System.out.println("  nodeID="+nodeID+" has postings");
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

        System.out.println("  visitNode nodeID="+nodeID+" splitLeafOrdinal="+splitLeafOrdinal);

        index.pushLeft();
        collectPostingLists(cellMinPacked, splitLeafOrdinal, index, visitor, resultNodeIDs);
        index.pop();

        index.pushRight();
        collectPostingLists(splitLeafOrdinal, cellMaxPacked, index, visitor, resultNodeIDs);
        index.pop();
    }

    // do a start range query, then an end range query and return the min and max leaf id's
    public Pair<Integer,Integer> traverseForMinMaxLeafOrdinals(ByteComparable start, ByteComparable end) throws IOException
    {
        int minLeafOrdinal = 0, maxLeafOrdinal = this.meta.numLeaves - 1;

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
                if (iterator.hasNext())
                {
                    Pair<ByteSource, Long> pair = iterator.next();
                    long value = pair.right.longValue();
                    int minLeaf = (int) (value >> 32);
                    int maxLeaf = (int) value;
                    minLeafOrdinal = minLeaf;
                }
                else
                {
                    minLeafOrdinal = this.meta.numLeaves;
                }
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
                if (iterator.hasNext())
                {
                    Pair<ByteSource, Long> pair = iterator.next();

                    long value = pair.right.longValue();
                    int minLeaf = (int) (value >> 32);
                    int maxLeaf = (int) value;

                    byte[] bytes = ByteSourceInverse.readBytes(pair.left);
                    if (ByteComparable.compare(ByteComparable.fixedLength(bytes), end, ByteComparable.Version.OSS41) > 0)
                    {
                        // if the term found is greater than what we're looking for, use the previous leaf
                        maxLeafOrdinal = minLeaf - 1;
                    }
                    else
                    {
                        maxLeafOrdinal = maxLeaf;
                    }
                    System.out.println("maxFoundTerm=" + NumericUtils.sortableBytesToInt(bytes, 0) +
                                       " minLeaf=" + minLeaf +
                                       " maxLeaf=" + maxLeaf);
                }
                else
                {
                    System.out.println("traverseForMinMaxLeafOrdinals no max term ");
                }
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

            if (leafOrdinal != this.leaf)
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

        //System.out.println("arraysFilePointer="+arraysFilePointer+" lengthsBytesLen="+lengthsBytesLen+" prefixBytesLen="+prefixBytesLen+" lengthsBits="+lengthsBits+" prefixBits="+prefixBits);

        lengthsReader = DirectReaders.getReaderForBitsPerValue(lengthsBits);
        prefixesReader = DirectReaders.getReaderForBitsPerValue(prefixBits);

        input.seek(arraysFilePointer + lengthsBytesLen + prefixBytesLen);

        leafBytesStartFP = leafBytesFP = input.getFilePointer();

        this.leafIndex = 0;
    }

    public BytesRef seekInBlock(int seekIndex) throws IOException
    {
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

        // start from where we left off
        if (seekIndex >= leafIndex)
        {
            start = leafIndex;
        }

        for (int x = start; x <= seekIndex; x++)
        {
            len = LeafOrderMap.getValue(seekingInput, arraysFilePointer, x, lengthsReader);
            prefix = LeafOrderMap.getValue(seekingInput, arraysFilePointer + lengthsBytesLen, x, prefixesReader);

            //System.out.println("x="+x+" len="+len+" prefix="+prefix);

            if (x == 0)
            {
                firstTerm = new byte[len];
                input.seek(leafBytesStartFP);
                input.readBytes(firstTerm, 0, len);
                lastPrefix = len;
                //System.out.println("firstTerm="+new BytesRef(firstTerm).utf8ToString());
                bytesLength = 0;
                leafBytesFP += len;
            }

            if (len > 0 && x > 0)
            {
                bytesLength = len - prefix;
                lastLen = len;
                lastPrefix = prefix;
                //System.out.println("x=" + x + " bytesLength=" + bytesLength + " len=" + len + " prefix=" + prefix);
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

            //System.out.println("bytesPosition=" + leafBytesFP + " bytesPositionStart=" + leafBytesStartFP + " total=" + (leafBytesFP - leafBytesStartFP));

            //System.out.println("lastlen=" + lastLen + " lastPrefix=" + lastPrefix + " bytesLength=" + bytesLength);
            final byte[] bytes = new byte[bytesLength];
            input.seek(leafBytesFP);
            input.readBytes(bytes, 0, bytesLength);

            leafBytesFP += bytesLength;

            //System.out.println("bytes read=" + new BytesRef(bytes).utf8ToString());

            builder.append(firstTerm, 0, lastPrefix);
            builder.append(bytes, 0, bytes.length);
        }

        //System.out.println("term="+builder.get().utf8ToString());
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
