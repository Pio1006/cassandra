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
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import com.carrotsearch.hppc.IntLongHashMap;
import org.agrona.collections.LongArrayList;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.io.tries.IncrementalDeepTrieWriterPageAware;
import org.apache.cassandra.io.tries.IncrementalTrieWriter;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.store.GrowableByteArrayDataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.packed.DirectWriter;

import static org.apache.cassandra.index.sai.disk.v1.BlockIndexReader.fixedLength;
import static org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat.BLOCK_SIZE;

public class BlockIndexWriter
{
    public static final int LEAF_SIZE = 2;

    // TODO: when the previous leaf min value is the same,
    //       write the leaf file pointer to the first occurence of the min value
    private final LongArrayList leafFilePointers = new LongArrayList();
    private final LongArrayList realLeafFilePointers = new LongArrayList();
    private final GrowableByteArrayDataOutput scratchOut = new GrowableByteArrayDataOutput(8 * 1024);
    private final GrowableByteArrayDataOutput prefixScratchOut = new GrowableByteArrayDataOutput(8 * 1024);
    private final GrowableByteArrayDataOutput lengthsScratchOut = new GrowableByteArrayDataOutput(8 * 1024);
    final int[] lengths = new int[LEAF_SIZE];
    final int[] prefixes = new int[LEAF_SIZE];
    final long[] postings = new long[LEAF_SIZE];
    final List<BytesRef> blockMinValues = new ArrayList();
    final BytesRefBuilder termBuilder = new BytesRefBuilder();
    final BytesRefBuilder lastTermBuilder = new BytesRefBuilder();
    final IndexOutput out;
    private int leafOrdinal;
    private int leaf;

    final IndexOutputWriter indexOut;
    final IndexOutput postingsOut, orderMapOut;

    final BitSet leafAllValuesSame = new BitSet();

    private final PostingsWriter postingsWriter;
    private final TreeMap<Integer,Long> leafToPostingsFP = new TreeMap();
    private final TreeMap<Integer,Long> leafToOrderMapFP = new TreeMap();
    final RangeSet<Integer> multiBlockLeafOrdinalRanges = TreeRangeSet.create();

    public BlockIndexWriter(IndexOutput out,
                            IndexOutputWriter indexOut,
                            IndexOutput postingsOut,
                            IndexOutput orderMapOut) throws IOException
    {
        this.out = out;
        this.indexOut = indexOut;
        this.postingsOut = postingsOut;
        this.orderMapOut = orderMapOut;

        postingsWriter = new PostingsWriter(postingsOut, BLOCK_SIZE, false);
    }

    public static class BlockIndexMeta
    {
        public final long orderMapFP;
        public final long indexFP;
        public final long leafFilePointersFP;
        public final int numLeaves;
        public final long nodeIDToLeafOrdinalFP;
        public final IntLongHashMap nodeIDPostingsFP;
        public final RangeSet<Integer> multiBlockLeafOrdinalRanges;

        public BlockIndexMeta(long orderMapFP, long indexFP, long leafFilePointersFP, int numLeaves, long nodeIDToLeafOrdinalFP, IntLongHashMap nodeIDPostingsFP, RangeSet<Integer> multiBlockLeafOrdinalRanges)
        {
            this.orderMapFP = orderMapFP;
            this.indexFP = indexFP;
            this.leafFilePointersFP = leafFilePointersFP;
            this.numLeaves = numLeaves;
            this.nodeIDToLeafOrdinalFP = nodeIDToLeafOrdinalFP;
            this.nodeIDPostingsFP = nodeIDPostingsFP;
            this.multiBlockLeafOrdinalRanges = multiBlockLeafOrdinalRanges;
        }
    }

    public BlockIndexMeta finish() throws IOException
    {

        if (leafOrdinal > 0) // if there are buffered values write a new leaf
        {
            writeLeaf();
        }

        // write the block min values index
        IncrementalTrieWriter termsIndexWriter = new IncrementalDeepTrieWriterPageAware<>(TrieTermsDictionaryReader.trieSerializer, indexOut.asSequentialWriter());
        int distinctCount = 0;
        int start = 0;
        int leafIdx = 0;
        // write distinct min block terms and the min and max leaf id's encoded as a long
        for (leafIdx = 0; leafIdx < blockMinValues.size(); leafIdx++)
        {
            BytesRef minValue = blockMinValues.get(leafIdx);
            if (leafIdx > 0)
            {
                BytesRef prevMinValue = blockMinValues.get(leafIdx - 1);
                if (!minValue.equals(prevMinValue))
                {
                    int startLeaf = start;
                    int endLeaf = leafIdx - 1;
                    System.out.println("termsIndexWriter write term="+prevMinValue.utf8ToString()+" startLeaf="+startLeaf+" endLeaf="+endLeaf);
                    long encodedLong = (((long)startLeaf) << 32) | (endLeaf & 0xffffffffL);
                    // TODO: when the start and end leaf's are the same encode a single int
                    termsIndexWriter.add(fixedLength(prevMinValue), new Long(encodedLong));
                    distinctCount = 0;
                    start = leafIdx;
                }
            }
            if (leafIdx == blockMinValues.size() - 1)
            {
                int endLeaf = leafIdx;
                BytesRef prevMinValue = blockMinValues.get(leafIdx);
                System.out.println("termsIndexWriter write2 term="+prevMinValue.utf8ToString()+" start="+start+" endLeaf="+endLeaf);
                long encodedLong = (((long)start) << 32) | (endLeaf & 0xffffffffL);
                termsIndexWriter.add(fixedLength(prevMinValue), new Long(encodedLong));
            }
            distinctCount++;
        }

        // write the last leaf's postings
        if (multiBlockStartLeaf != -1 && writingMultiBlock)
        {
            final long postingsFP = postingsWriter.completePostings();

            assert multiBlockStartLeaf >= 0;

            leafToPostingsFP.put(multiBlockStartLeaf, postingsFP);

            if (multiBlockStartLeaf != leaf - 1)
            {
                Range range = Range.closed(multiBlockStartLeaf, leaf - 1);
                System.out.println("range=" + range);
                multiBlockLeafOrdinalRanges.add(range);
            }

            multiBlockStartLeaf = -1;
            writingMultiBlock = false;
        }

        assert leafFilePointers.size() == blockMinValues.size()
        : "leafFilePointers.size=" + leafFilePointers.size() + " blockMinValues.size=" + blockMinValues.size();

        final int numLeaves = leafFilePointers.size();

        System.out.println("numLeaves="+numLeaves);

        final long leafFilePointersFP = out.getFilePointer();

        for (int x=0; x < realLeafFilePointers.size(); x++)
        {
            out.writeVLong(realLeafFilePointers.get(x));
        }

        final TreeMap<Integer,Integer> nodeIDToLeafOrdinal = new TreeMap();

        rotateToTree(1, 0, leafFilePointers.size() - 1, nodeIDToLeafOrdinal);

        System.out.println("leafFilePointers.size=" + leafFilePointers.size() + " nodeIDToLeafOrdinal=" + nodeIDToLeafOrdinal);

        final TreeMap<Integer, Long> nodeIDToLeafPointer = new TreeMap<>();

        long[] leafBlockFPs = leafFilePointers.toLongArray();

        assert numLeaves == leafBlockFPs.length;

        // this is wacky lucene code that rearranges the leaf file pointers
        if (numLeaves > 1)
        {
            int levelCount = 2;
            while (true)
            {
                if (numLeaves >= levelCount && numLeaves <= 2 * levelCount)
                {
                    int lastLevel = 2 * (numLeaves - levelCount);
                    assert lastLevel >= 0;
                    if (lastLevel != 0)
                    {
                        // Last level is partially filled, so we must rotate the leaf FPs to match.  We do this here, after loading
                        // at read-time, so that we can still delta code them on disk at write:
                        long[] newLeafBlockFPs = new long[numLeaves];
                        System.arraycopy(leafBlockFPs, lastLevel, newLeafBlockFPs, 0, leafBlockFPs.length - lastLevel);
                        System.arraycopy(leafBlockFPs, 0, newLeafBlockFPs, leafBlockFPs.length - lastLevel, lastLevel);
                        leafBlockFPs = newLeafBlockFPs;
                    }
                    break;
                }

                levelCount *= 2;
            }
        }

        // traverse to gather into nodeIDToLeafPointer
        recursePackIndex(leafBlockFPs,
                         0l,
                         1,
                         true,
                         nodeIDToLeafPointer);

        System.out.println("nodeIDToLeafPointer="+nodeIDToLeafPointer);

        // TODO: the "leafPointer" is actually the leaf id because
        //       the binary tree code requires unique values
        //       due to the same block min values having same file pointer
        //       the actual leaf file pointer can't be used here
        final TreeMap<Long, Integer> leafPointerToNodeID = new TreeMap<>();
        for (Map.Entry<Integer,Long> entry : nodeIDToLeafPointer.entrySet())
        {
            leafPointerToNodeID.put(entry.getValue(), entry.getKey());
        }

        int ordinal = 0;
        for (Map.Entry<Long, Integer> entry : leafPointerToNodeID.entrySet())
        {
            nodeIDToLeafOrdinal.put(entry.getValue(), ordinal);
            ordinal++;
        }

        System.out.println("nodeIDToLeafOrdinal2="+nodeIDToLeafOrdinal);

        final IntLongHashMap nodeIDPostingsFP = new IntLongHashMap();

        final long nodeIDToLeafOrdinalFP = out.getFilePointer();
        out.writeVInt(nodeIDToLeafOrdinal.size());

        for (Map.Entry<Integer, Integer> entry : nodeIDToLeafOrdinal.entrySet())
        {
            out.writeVInt(entry.getKey());
            out.writeVInt(entry.getValue());

            int nodeID = entry.getKey();
            int leafOrdinal = entry.getValue();

            if (nodeID >= numLeaves)
            {
                final Long postingsFP = leafToPostingsFP.get(leafOrdinal);

                if (postingsFP != null)
                {
                    nodeIDPostingsFP.put(nodeID, postingsFP);
                }
            }

//            final Long postingsFP = leafToPostingsFP.get(leafOrdinal);
//            if (postingsFP != null)
//            {
//                nodeIDPostingsFP.put(nodeID, postingsFP);
//            }
        }

        final long orderMapFP = this.orderMapOut.getFilePointer();
        orderMapOut.writeVInt(this.leafToOrderMapFP.size());
        for (Map.Entry<Integer, Long> entry : this.leafToOrderMapFP.entrySet())
        {
            orderMapOut.writeVInt(entry.getKey());
            orderMapOut.writeVLong(entry.getValue());
        }

        final long indexFP = termsIndexWriter.complete();

        System.out.println("leafToPostingsFP=" + leafToPostingsFP);

        termsIndexWriter.close();
        indexOut.close();
        orderMapOut.close();
        out.close();

        return new BlockIndexMeta(orderMapFP,
                                  indexFP,
                                  leafFilePointersFP,
                                  leafFilePointers.size(),
                                  nodeIDToLeafOrdinalFP,
                                  nodeIDPostingsFP,
                                  multiBlockLeafOrdinalRanges);
    }

    public static int bytesDifference(BytesRef priorTerm, BytesRef currentTerm) {
        int mismatch = FutureArrays.mismatch(priorTerm.bytes, priorTerm.offset, priorTerm.offset + priorTerm.length, currentTerm.bytes, currentTerm.offset, currentTerm.offset + currentTerm.length);
        return mismatch;
    }

    final BytesRefBuilder lastAddedTerm = new BytesRefBuilder();
    private boolean allLeafValuesSame = true;

    public void add(ByteComparable term, long rowID) throws IOException
    {
        int length = 0;
        final ByteSource byteSource = term.asComparableBytes(ByteComparable.Version.OSS41);

        termBuilder.clear();

        // gather the term bytes from the byteSource
        while (true)
        {
            final int val = byteSource.next();
            if (val != ByteSource.END_OF_STREAM)
            {
                ++length;
                termBuilder.append((byte)val);
            }
            else
            {
                break;
            }
        }

        if (leafOrdinal > 0 && !termBuilder.get().equals(lastAddedTerm.get()))
        {
            allLeafValuesSame = false;
        }

        lastAddedTerm.clear();
        lastAddedTerm.append(termBuilder.get());

        if (lastTermBuilder.length() == 0)
        {
            assert leafOrdinal == 0;
            lastTermBuilder.append(termBuilder);
            prefixes[leafOrdinal] = 0;
            lengths[leafOrdinal] = termBuilder.get().length;
            blockMinValues.add(BytesRef.deepCopyOf(termBuilder.get()));
        }
        else
        {
            //System.out.println("prefix=" + lastTermBuilder.get().utf8ToString() + " term=" + termBuilder.get().utf8ToString());
            int prefix = bytesDifference(lastTermBuilder.get(), termBuilder.get());
            if (prefix == -1) prefix = length;
            prefixes[leafOrdinal] = prefix;
            lengths[leafOrdinal] = termBuilder.get().length;
        }
        System.out.println("term=" + termBuilder.get().utf8ToString() + " prefix=" + prefixes[leafOrdinal] + " length=" + lengths[leafOrdinal]);

        int prefix = prefixes[leafOrdinal];
        int len = termBuilder.get().length - prefixes[leafOrdinal];

        if (leafOrdinal == 0)
        {
            prefixes[leafOrdinal] = termBuilder.get().length;
        }

        //System.out.println("write leafIndex=" + leafOrdinal + " prefix=" + prefix + " len=" + len);
        scratchOut.writeBytes(termBuilder.get().bytes, prefix, len);

        postings[leafOrdinal] = rowID;

        leafOrdinal++;

        if (leafOrdinal == LEAF_SIZE)
        {
            writeLeaf();
            leafOrdinal = 0;
            scratchOut.reset();
            prefixScratchOut.reset();
            lengthsScratchOut.reset();
            termBuilder.clear();
            lastTermBuilder.clear();
        }
    }

    protected void writeLeaf() throws IOException
    {
        final BytesRef minValue = blockMinValues.get(leaf);
        this.leafFilePointers.add((long)leaf);

        if (allLeafValuesSame)
        {
            leafAllValuesSame.set(leaf);
        }

        System.out.println("  writeLeaf leaf="+leaf+" minValue="+minValue.utf8ToString()+" allLeafValuesSame="+allLeafValuesSame);

        try
        {
            if (leaf > 0)
            {
                // if the previous min value is the same as this leaf's
                // then simply refer to the previous leaf's bytes by using the same file pointer
                final BytesRef prevMinValue = blockMinValues.get(leaf - 1);
                if (minValue.equals(prevMinValue) && allLeafValuesSame)
                {
                    System.out.println("duplicate minValue=" + minValue.utf8ToString());

                    long previousRealFP = this.realLeafFilePointers.get(this.realLeafFilePointers.size() - 1);
                    this.realLeafFilePointers.add(previousRealFP);
                    return;
                }
            }

            long filePointer = out.getFilePointer();
            System.out.println("writeLeaf leaf=" + leaf + " filePointer=" + filePointer + " leafIndex=" + leafOrdinal);
            final int maxLength = Arrays.stream(lengths).max().getAsInt();
            LeafOrderMap.write(lengths, leafOrdinal, maxLength, lengthsScratchOut);
            final int maxPrefix = Arrays.stream(prefixes).max().getAsInt();
            //System.out.println("prefixes=" + Arrays.toString(Arrays.copyOf(prefixes, leafOrdinal)));
            LeafOrderMap.write(prefixes, leafOrdinal, maxPrefix, prefixScratchOut);

            out.writeInt(leafOrdinal); // value count
            out.writeInt(lengthsScratchOut.getPosition());
            out.writeInt(prefixScratchOut.getPosition());
            out.writeByte((byte) DirectWriter.unsignedBitsRequired(maxLength));
            out.writeByte((byte) DirectWriter.unsignedBitsRequired(maxPrefix));
            out.writeBytes(lengthsScratchOut.getBytes(), 0, lengthsScratchOut.getPosition());
            out.writeBytes(prefixScratchOut.getBytes(), 0, prefixScratchOut.getPosition());
            //System.out.println("write bytes file pointer=" + out.getFilePointer());
            out.writeBytes(scratchOut.getBytes(), 0, scratchOut.getPosition());

            this.realLeafFilePointers.add(filePointer);
        }
        finally
        {
            writePostings(allLeafValuesSame);

            lastAddedTerm.clear();
            allLeafValuesSame = true;

            leaf++;
        }
    }

    private boolean writingMultiBlock = false;
    private int multiBlockStartLeaf = -1;

    public static class RowIDLeafOrdinal
    {
        public int leafOrdinal;
        public long rowID;
    }

    final RowIDLeafOrdinal[] rowIDLeafOrdinals = new RowIDLeafOrdinal[LEAF_SIZE];
    {
        for (int x = 0; x < rowIDLeafOrdinals.length; x++)
        {
            rowIDLeafOrdinals[x] = new RowIDLeafOrdinal();
        }
    }

    protected void writePostings(boolean allLeafValuesSame) throws IOException
    {
        assert blockMinValues.size() - 1 == leaf : "blockMinValues.size="+blockMinValues.size()+" leaf="+leaf;

        assert leafOrdinal > 0;

        boolean closePostings = false;

        final int initialMultiBlockStartLeaf = multiBlockStartLeaf;

        if (allLeafValuesSame)
        {
            if (leaf > 0)
            {
                BytesRef prevMinVal = blockMinValues.get(leaf - 1);
                BytesRef minVal = blockMinValues.get(leaf);
                if (!prevMinVal.equals(minVal))
                {
                    assert postingsWriter.getTotalPostings() > 0;
                    if (writingMultiBlock)
                    {
                        closePostings = true;
                    }
                }
                else
                {

                }
                if (multiBlockStartLeaf == -1)
                {
                    multiBlockStartLeaf = leaf;
                }
            }
            if (!writingMultiBlock)
            {
                postingsWriter.initPostings();
                multiBlockStartLeaf = leaf;
                writingMultiBlock = true;
            }
        }
        else
        {
            multiBlockStartLeaf = -1;
            closePostings = true;
        }

        if (initialMultiBlockStartLeaf != -1 && closePostings)
        {
            final long filePointer = postingsWriter.completePostings();

            assert initialMultiBlockStartLeaf >= 0;

            leafToPostingsFP.put(initialMultiBlockStartLeaf, filePointer);
            System.out.println("leafToPostingsFP initialMultiBlockStartLeaf="+initialMultiBlockStartLeaf+" fp="+filePointer);

            System.out.println("initialMultiBlockStartLeaf="+initialMultiBlockStartLeaf+" leaf-1="+(leaf-1));

            if (initialMultiBlockStartLeaf < leaf - 1)
            {
                Range range = Range.closed(initialMultiBlockStartLeaf, leaf - 1);
                System.out.println("writePostings range=" + range);
                multiBlockLeafOrdinalRanges.add(range);
                for (int x=initialMultiBlockStartLeaf + 1; x <= (leaf - 1); x++)
                {
                    System.out.println("leafToPostingsFP leaf="+x+" fp="+filePointer);
                    leafToPostingsFP.put(x, filePointer);
                }
            }

            multiBlockStartLeaf = -1;
            writingMultiBlock = false;
            closePostings = false;
        }

        if (!writingMultiBlock)
        {
            if (leaf > 0)
            {

            }
            postingsWriter.initPostings();
        }

        for (int x = 0; x < leafOrdinal; x++)
        {
            rowIDLeafOrdinals[x].rowID = postings[x];
            rowIDLeafOrdinals[x].leafOrdinal = x;
        }

        // sort by row id
        Arrays.sort(rowIDLeafOrdinals, 0, leafOrdinal, (obj1, obj2) -> Long.compare(obj1.rowID, obj2.rowID));

        // write the sorted postings to the postings writer
        for (int x = 0; x < leafOrdinal; x++)
        {
            postingsWriter.add(rowIDLeafOrdinals[x].rowID);
        }

        // write order map
        final long orderMapFP = orderMapOut.getFilePointer();
        final int bits = DirectWriter.unsignedBitsRequired(LEAF_SIZE - 1);
        final DirectWriter writer = DirectWriter.getInstance(orderMapOut, leafOrdinal, bits);
        for (int i = 0; i < leafOrdinal; i++)
        {
            writer.add(rowIDLeafOrdinals[i].leafOrdinal);
        }
        writer.finish();
        leafToOrderMapFP.put(leaf, orderMapFP);

        // if not writing multi-block postings, close the leaf posting list
        if (!writingMultiBlock)
        {
            long filePointer = postingsWriter.completePostings();
            leafToPostingsFP.put(leaf, filePointer);
            multiBlockStartLeaf = -1;
        }

        System.out.println("writePostings end leaf="+leaf+" multiBlockStartLeaf="+multiBlockStartLeaf);
    }

    private long getLeftMostLeafBlockFP(long[] leafBlockFPs, int nodeID)
    {
        // TODO: can we do this cheaper, e.g. a closed form solution instead of while loop?  Or
        // change the recursion while packing the index to return this left-most leaf block FP
        // from each recursion instead?
        //
        // Still, the overall cost here is minor: this method's cost is O(log(N)), and while writing
        // we call it O(N) times (N = number of leaf blocks)
        while (nodeID < leafBlockFPs.length)
        {
            nodeID *= 2;
        }
        int leafID = nodeID - leafBlockFPs.length;
        long result = leafBlockFPs[leafID];
        if (result < 0)
        {
            throw new AssertionError(result + " for leaf " + leafID);
        }
        return result;
    }

    private void rotateToTree(int nodeID, int offset, int count, Map<Integer,Integer> nodeIDToLeafOrdinal)
    {
        //System.out.println("ROTATE: nodeID=" + nodeID + " offset=" + offset + " count=" + count + " bpd=" + bytesPerDim + " index.length=" + index.length);
        if (count == 1)
        {
            // Leaf index node
            //System.out.println("  leaf index node");
            //System.out.println("  index[" + nodeID + "] = blockStartValues[" + offset + "]");

            nodeIDToLeafOrdinal.put(nodeID, offset + 1);

            //System.arraycopy(leafBlockStartValues.get(offset), 0, index, nodeID * (1 + bytesPerDim) + 1, bytesPerDim);
        }
        else if (count > 1)
        {
            // Internal index node: binary partition of count
            int countAtLevel = 1;
            int totalCount = 0;
            while (true)
            {
                int countLeft = count - totalCount;
                //System.out.println("    cycle countLeft=" + countLeft + " coutAtLevel=" + countAtLevel);
                if (countLeft <= countAtLevel)
                {
                    // This is the last level, possibly partially filled:
                    int lastLeftCount = Math.min(countAtLevel / 2, countLeft);
                    assert lastLeftCount >= 0;
                    int leftHalf = (totalCount - 1) / 2 + lastLeftCount;

                    int rootOffset = offset + leftHalf;
          /*
          System.out.println("  last left count " + lastLeftCount);
          System.out.println("  leftHalf " + leftHalf + " rightHalf=" + (count-leftHalf-1));
          System.out.println("  rootOffset=" + rootOffset);
          */

                    nodeIDToLeafOrdinal.put(nodeID, rootOffset + 1);

                    //System.arraycopy(leafBlockStartValues.get(rootOffset), 0, index, nodeID * (1 + bytesPerDim) + 1, bytesPerDim);
                    //System.out.println("  index[" + nodeID + "] = blockStartValues[" + rootOffset + "]");

                    // TODO: we could optimize/specialize, when we know it's simply fully balanced binary tree
                    // under here, to save this while loop on each recursion

                    // Recurse left
                    rotateToTree(2 * nodeID, offset, leftHalf, nodeIDToLeafOrdinal);

                    // Recurse right
                    rotateToTree(2 * nodeID + 1, rootOffset + 1, count - leftHalf - 1, nodeIDToLeafOrdinal);
                    return;
                }
                totalCount += countAtLevel;
                countAtLevel *= 2;
            }
        }
        else
        {
            assert count == 0;
        }
    }

    private void recursePackIndex(long[] leafBlockFPs,
                                  long minBlockFP,
                                  int nodeID,
                                  boolean isLeft,
                                  TreeMap<Integer,Long> nodeIDToLeafPointer) throws IOException
    {
        if (nodeID >= leafBlockFPs.length)
        {
            int leafID = nodeID - leafBlockFPs.length;
            long fakeFP = leafBlockFPs[leafID];

            // In the unbalanced case it's possible the left most node only has one child:
            if (leafID < leafBlockFPs.length)
            {
                nodeIDToLeafPointer.put(nodeID, fakeFP);
                return;
            }
            return;
        }
        else
        {
            long leftBlockFP;
            if (isLeft == false)
            {
                leftBlockFP = getLeftMostLeafBlockFP(leafBlockFPs, nodeID);
            }
            else
            {
                // The left tree's left most leaf block FP is always the minimal FP:
                leftBlockFP = minBlockFP;
            }

            recursePackIndex(leafBlockFPs, leftBlockFP, 2 * nodeID,true, nodeIDToLeafPointer);

            recursePackIndex(leafBlockFPs, leftBlockFP, 2 * nodeID + 1, false, nodeIDToLeafPointer);

            return;
        }
    }
}
