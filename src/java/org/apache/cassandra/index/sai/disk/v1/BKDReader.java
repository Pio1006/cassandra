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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.collections.LongArrayList;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.io.CryptoUtils;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.AbstractIterator;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.packed.DirectWriter;

/**
 * Handles intersection of a multi-dimensional shape in byte[] space with a block KD-tree previously written with
 * {@link BKDWriter}.
 */
public class BKDReader extends TraversingBKDReader implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Comparator<PostingList.PeekablePostingList> COMPARATOR = Comparator.comparingLong(PostingList.PeekablePostingList::peek);

    private final FileHandle postingsFile, kdtreeFile;
    private final BKDPostingsIndex postingsIndex;
    private final ICompressor compressor;
    private final DirectReaders.Reader leafOrderMapReader;

    /**
     * Performs a blocking read.
     */
    public BKDReader(IndexComponents indexComponents,
                     FileHandle kdtreeFile,
                     long bkdIndexRoot,
                     FileHandle postingsFile,
                     long bkdPostingsRoot) throws IOException
    {
        super(indexComponents, kdtreeFile, bkdIndexRoot);
        this.postingsFile = postingsFile;
        this.kdtreeFile = kdtreeFile;
        this.postingsIndex = new BKDPostingsIndex(postingsFile, bkdPostingsRoot);
        this.compressor = null;
        final byte bits = (byte) DirectWriter.unsignedBitsRequired(maxPointsInLeafNode - 1);
        leafOrderMapReader = DirectReaders.getReaderForBitsPerValue(bits);
    }

    public interface DocMapper
    {
        long oldToNew(long rowID);
    }

    private TreeMap<Long,Integer> getLeafOffsets()
    {
        final TreeMap<Long,Integer> map = new TreeMap();
        final PackedIndexTree index = new PackedIndexTree();
        getLeafOffsets(index, map);
        return map;
    }

    private void getLeafOffsets(final IndexTree index, TreeMap<Long, Integer> map)
    {
        if (index.isLeafNode())
        {
            if (index.nodeExists())
            {
                map.put(index.getLeafBlockFP(), index.getNodeID());
            }
        }
        else
        {
            index.pushLeft();
            getLeafOffsets(index, map);
            index.pop();

            index.pushRight();
            getLeafOffsets(index, map);
            index.pop();
        }
    }

    @VisibleForTesting
    public IteratorState iteratorState() throws IOException
    {
        return new IteratorState((rowID) -> rowID);
    }

    public IteratorState iteratorState(DocMapper docMapper) throws IOException
    {
        return new IteratorState(docMapper);
    }

    public TreeMap<Integer,Integer> nodeIDToLeafOrdinal()
    {
        final TreeMap<Long,Integer> leafFPToLeafNodeID = getLeafOffsets();

        TreeMap<Integer,Integer> nodeIDToLeafOrdinal = new TreeMap();

        Map.Entry<Long,Integer> first = leafFPToLeafNodeID.pollFirstEntry();

        int i = 1;
        for (int nodeID : leafFPToLeafNodeID.values())
        {
            nodeIDToLeafOrdinal.put(nodeID, i);
            i++;
        }

        rotateToTree(1, 0, nodeIDToLeafOrdinal.size(), nodeIDToLeafOrdinal);

        nodeIDToLeafOrdinal.put(first.getValue(), 0);

        System.out.println("nodeIDToLeafOrdinal=" + nodeIDToLeafOrdinal);

        TreeSet<Integer> leafOrdinals = new TreeSet(nodeIDToLeafOrdinal.values());

        System.out.println("leafOrdinals=" + leafOrdinals);

        return nodeIDToLeafOrdinal;
    }

    public Set<Integer> traverseCheck(int minLeaf, int maxLeaf) throws IOException
    {
        TreeMap<Integer,Integer> nodeIDToLeafOrdinal = nodeIDToLeafOrdinal();
        TreeSet<Integer> leafOrdinals = new TreeSet(nodeIDToLeafOrdinal.values());

        final PackedIndexTree index = new PackedIndexTree();

        SimpleRangeVisitor visitor = new SimpleRangeVisitor(new SimpleBound(minLeaf, true), new SimpleBound(maxLeaf, false));

        Set<Integer> resultNodeIDs = new TreeSet();

        int maxLeafOrdinal = leafOrdinals.last();

        collectPostingLists(0,
                            maxLeafOrdinal,
                            index,
                            nodeIDToLeafOrdinal,
                            visitor,
                            resultNodeIDs);

        System.out.println("resultNodeIDs="+resultNodeIDs);

        return resultNodeIDs;
    }

    interface SimpleVisitor
    {
        Relation compare(int minOrdinal, int maxOrdinal);
    }

    public static class SimpleBound
    {
        public final int bound;
        public final boolean exclusive;

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
       final SimpleBound lower, upper;

        public SimpleRangeVisitor(SimpleBound lower, SimpleBound upper)
        {
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public Relation compare(int minValue, int maxValue)
        {
            boolean crosses = false;

            if (lower != null)
            {
//                int maxCmp = compareUnsigned(maxPackedValue, dim, lower);
//                if (lower.greaterThan(maxCmp))
//                    return Relation.CELL_OUTSIDE_QUERY;
//
//                int minCmp = compareUnsigned(minPackedValue, dim, lower);
//                crosses |= lower.greaterThan(minCmp);

                int maxCmp = Integer.compare(maxValue, lower.bound);
                if (lower.greaterThan(maxCmp))
                    return Relation.CELL_OUTSIDE_QUERY;

                int minCmp = Integer.compare(minValue, lower.bound);
                crosses |= lower.greaterThan(minCmp);
            }

//            int minCmp = compareUnsigned(minPackedValue, dim, upper);
//            if (upper.smallerThan(minCmp))
//                return Relation.CELL_OUTSIDE_QUERY;
//
//            int maxCmp = compareUnsigned(maxPackedValue, dim, upper);
//            crosses |= upper.smallerThan(maxCmp);
            if (upper != null)
            {
                int minCmp = Integer.compare(minValue, upper.bound);
                if (upper.smallerThan(minCmp))
                    return Relation.CELL_OUTSIDE_QUERY;

                int maxCmp = Integer.compare(maxValue, upper.bound);
                crosses |= upper.smallerThan(maxCmp);
            }

            if (crosses)
            {
                return Relation.CELL_CROSSES_QUERY;
            }
            else
            {
                return Relation.CELL_INSIDE_QUERY;
            }
        }
    }

    public void collectPostingLists(int cellMinLeafOrdinal,
                                    int cellMaxLeafOrdinal,
                                    final IndexTree index,
                                    TreeMap<Integer,Integer> nodeIDToLeafOrdinal,
                                    SimpleVisitor visitor,
                                    Set<Integer> resultNodeIDs) throws IOException
    {
        final Relation r = visitor.compare(cellMinLeafOrdinal, cellMaxLeafOrdinal);

        if (r == Relation.CELL_OUTSIDE_QUERY)
        {
            // This cell is fully outside of the query shape: stop recursing
            return;
        }

        if (r == Relation.CELL_INSIDE_QUERY)
        {
            final int nodeID = index.getNodeID();

            // if there is pre-built posting for entire subtree
            if (postingsIndex.exists(nodeID))
            {
                resultNodeIDs.add(nodeID);
                return;
            }

            Preconditions.checkState(!index.isLeafNode(), "Leaf node %s does not have kd-tree postings.", index.getNodeID());

            visitNode(cellMinLeafOrdinal,
                      cellMaxLeafOrdinal,
                      index,
                      nodeIDToLeafOrdinal,
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
                  nodeIDToLeafOrdinal,
                  visitor,
                  resultNodeIDs);
    }

    void visitNode(int cellMinPacked,
                   int cellMaxPacked,
                   final IndexTree index,
                   TreeMap<Integer,Integer> nodeIDToLeafOrdinal,
                   SimpleVisitor visitor,
                   Set<Integer> resultNodeIDs) throws IOException
    {
        int nodeID = index.getNodeID();
        int splitLeafOrdinal = nodeIDToLeafOrdinal.get(nodeID);

        index.pushLeft();
        collectPostingLists(cellMinPacked, splitLeafOrdinal, index, nodeIDToLeafOrdinal, visitor, resultNodeIDs);
        index.pop();

        index.pushRight();
        collectPostingLists(splitLeafOrdinal, cellMaxPacked, index, nodeIDToLeafOrdinal, visitor, resultNodeIDs);
        index.pop();
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

    public class IteratorState extends AbstractIterator<Long> implements Comparable<IteratorState>, Closeable
    {
        final IndexInput bkdInput;
        final IndexInput bkdPostingsInput;
        final byte[] packedValues = new byte[maxPointsInLeafNode * packedBytesLength];
        private int leaf, leafPointCount, leafPointIndex = -1;
        final LongArrayList tempPostings = new LongArrayList();
        final long[] postings = new long[maxPointsInLeafNode];
        final DocMapper docMapper;
        public final byte[] scratch;
        final Iterator<Map.Entry<Long,Integer>> iterator;

        public IteratorState(DocMapper docMapper) throws IOException
        {
            this.docMapper = docMapper;

            scratch = new byte[packedBytesLength];

            final long firstLeafFilePointer = getMinLeafBlockFP();
            bkdInput = indexComponents.openInput(kdtreeFile);
            bkdPostingsInput = indexComponents.openInput(postingsFile);
            bkdInput.seek(firstLeafFilePointer);

            final TreeMap<Long,Integer> leafNodeToLeafFP = getLeafOffsets();

            // init the first leaf
            iterator = leafNodeToLeafFP.entrySet().iterator();
            final Map.Entry<Long,Integer> entry = iterator.next();
            leafPointCount = readLeaf(entry.getKey(), entry.getValue(), bkdInput, packedValues, bkdPostingsInput, postings, tempPostings);
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(bkdInput, bkdPostingsInput);
        }

        @Override
        public int compareTo(final IteratorState other)
        {
            final int cmp = FutureArrays.compareUnsigned(scratch, 0, packedBytesLength, other.scratch, 0, packedBytesLength);
            if (cmp == 0)
            {
                final long rowid1 = next;
                final long rowid2 = other.next;
                return Long.compare(rowid1, rowid2);
            }
            return cmp;
        }

        @Override
        protected Long computeNext()
        {
            while (true)
            {
                if (leafPointIndex == leafPointCount - 1)
                {
                    leaf++;
                    if (leaf == numLeaves && leafPointIndex == leafPointCount - 1)
                    {
                        return endOfData();
                    }
                    final Map.Entry<Long, Integer> entry = iterator.next();
                    try
                    {
                        leafPointCount = readLeaf(entry.getKey(), entry.getValue(), bkdInput, packedValues, bkdPostingsInput, postings, tempPostings);
                    }
                    catch (IOException e)
                    {
                        logger.error("Failed to read leaf during BKDTree merger", e);
                        throw new RuntimeException("Failed to read leaf during BKDTree merger", e);
                    }
                    leafPointIndex = -1;
                }

                leafPointIndex++;

                System.arraycopy(packedValues, leafPointIndex * packedBytesLength, scratch, 0, packedBytesLength);
                return docMapper.oldToNew(postings[leafPointIndex]);
            }
        }
    }

    @SuppressWarnings("resource")
    public int readLeaf(long filePointer,
                        int nodeID,
                        final IndexInput bkdInput,
                        final byte[] packedValues,
                        final IndexInput bkdPostingsInput,
                        long[] postings,
                        LongArrayList tempPostings) throws IOException
    {
        bkdInput.seek(filePointer);
        final int count = bkdInput.readVInt();
        // loading doc ids occurred here prior
        final int orderMapLength = bkdInput.readVInt();
        final long orderMapPointer = bkdInput.getFilePointer();

        // order of the values in the posting list
        final short[] origIndex = new short[maxPointsInLeafNode];

        final int[] commonPrefixLengths = new int[numDims];
        final byte[] scratchPackedValue1 = new byte[packedBytesLength];

        final SeekingRandomAccessInput randoInput = new SeekingRandomAccessInput(bkdInput);
        for (int x = 0; x < count; x++)
        {
            final short idx = (short) LeafOrderMap.getValue(randoInput, orderMapPointer, x, leafOrderMapReader);
            origIndex[x] = idx;
        }

        IndexInput leafInput = bkdInput;

        // reused byte arrays for the decompression of leaf values
        final BytesRef uncompBytes = new BytesRef(new byte[16]);
        final BytesRef compBytes = new BytesRef(new byte[16]);

        // seek beyond the ordermap
        leafInput.seek(orderMapPointer + orderMapLength);

        if (compressor != null)
        {
            // This should not throw WouldBlockException, even though we're on a TPC thread, because the
            // secret key used by the underlying encryptor should be loaded at reader construction time.
            leafInput = CryptoUtils.uncompress(bkdInput, compressor, compBytes, uncompBytes);
        }

        final IntersectVisitor visitor = new IntersectVisitor() {
            int i = 0;

            @Override
            public boolean visit(byte[] packedValue)
            {
                System.arraycopy(packedValue, 0, packedValues, i * packedBytesLength, packedBytesLength);
                i++;
                return true;
            }

            @Override
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                return Relation.CELL_CROSSES_QUERY;
            }
        };

        visitDocValues(commonPrefixLengths, scratchPackedValue1, leafInput, count, visitor, null, origIndex);

        if (postingsIndex.exists(nodeID))
        {
            final long pointer = postingsIndex.getPostingsFilePointer(nodeID);
            final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(bkdPostingsInput, pointer);
            final PostingsReader postingsReader = new PostingsReader(bkdPostingsInput, summary, QueryEventListener.PostingListEventListener.NO_OP);

            tempPostings.clear();

            // gather the postings into tempPostings
            while (true)
            {
                final long rowid = postingsReader.nextPosting();
                if (rowid == PostingList.END_OF_STREAM) break;
                tempPostings.add(rowid);
            }

            // put the postings into the array according the origIndex
            for (int x = 0; x < tempPostings.size(); x++)
            {
                int idx = origIndex[x];
                final long rowid = tempPostings.get(idx);

                postings[x] = rowid;
            }
        }
        else
        {
            throw new IllegalStateException();
        }
        return count;
    }

    public static int openPerIndexFiles()
    {
        // kd-tree, posting lists file
        return 2;
    }

    @Override
    public void close()
    {
        try
        {
            super.close();
        }
        finally
        {
            postingsFile.close();
        }
    }

    @SuppressWarnings("resource")
    public PostingList intersect(IntersectVisitor visitor, QueryEventListener.BKDIndexEventListener listener, QueryContext context)
    {
        Relation relation = visitor.compare(minPackedValue, maxPackedValue);

        if (relation == Relation.CELL_OUTSIDE_QUERY)
        {
            listener.onIntersectionEarlyExit();
            return null;
        }

        listener.onSegmentHit();
        IndexInput bkdInput = indexComponents.openInput(indexFile);
        IndexInput postingsInput = indexComponents.openInput(postingsFile);
        IndexInput postingsSummaryInput = indexComponents.openInput(postingsFile);
        PackedIndexTree index = new PackedIndexTree();

        Intersection completable =
        relation == Relation.CELL_INSIDE_QUERY ?
                new Intersection(bkdInput, postingsInput, postingsSummaryInput, index, listener, context) :
                new FilteringIntersection(bkdInput, postingsInput, postingsSummaryInput, index, visitor, listener, context);

        return completable.execute();
    }

    /**
     * Synchronous intersection of an multi-dimensional shape in byte[] space with a block KD-tree
     * previously written with {@link BKDWriter}.
     */
    class Intersection
    {
        private final Stopwatch queryExecutionTimer = Stopwatch.createStarted();
        final QueryContext context;

        final IndexInput bkdInput;
        final IndexInput postingsInput;
        final IndexInput postingsSummaryInput;
        final IndexTree index;
        final QueryEventListener.BKDIndexEventListener listener;

        Intersection(IndexInput bkdInput, IndexInput postingsInput, IndexInput postingsSummaryInput,
                     IndexTree index, QueryEventListener.BKDIndexEventListener listener, QueryContext context)
        {
            this.bkdInput = bkdInput;
            this.postingsInput = postingsInput;
            this.postingsSummaryInput = postingsSummaryInput;
            this.index = index;
            this.listener = listener;
            this.context = context;
        }

        public PostingList execute()
        {
            try
            {
                PriorityQueue<PostingList.PeekablePostingList> postingLists = new PriorityQueue<>(100, COMPARATOR);

                Set<Integer> nodeIDs = new TreeSet<>();

                executeInternal(postingLists, nodeIDs);

                System.out.println("nodeIDs="+nodeIDs);

                TreeMap<Integer,Integer> nodeIDToLeafOrdinal = nodeIDToLeafOrdinal();
                int maxLeafOrdinal = -1;
                int minLeafOrdinal = Integer.MAX_VALUE;
                for (int nodeID : nodeIDs)
                {
                    int leafOrdinal = nodeIDToLeafOrdinal.get(nodeID);
                    maxLeafOrdinal = Math.max(maxLeafOrdinal, leafOrdinal);
                    minLeafOrdinal = Math.min(minLeafOrdinal, leafOrdinal);
                }

                System.out.println("minLeafOrdinal="+minLeafOrdinal+" maxLeafOrdinal="+maxLeafOrdinal);

                Set<Integer> nodeIDs2 = BKDReader.this.traverseCheck(minLeafOrdinal, maxLeafOrdinal);

                System.out.println("nodeIDs2="+nodeIDs2);

                FileUtils.closeQuietly(bkdInput);

                return mergePostings(postingLists);
            }
            catch (Throwable t)
            {
                if (!(t instanceof AbortedOperationException))
                    logger.error(indexComponents.logMessage("kd-tree intersection failed on {}"), indexFile.path(), t);

                closeOnException();
                throw Throwables.cleaned(t);
            }
        }

        protected void executeInternal(final PriorityQueue<PostingList.PeekablePostingList> postingLists,
                                       Set<Integer> nodeIDs) throws IOException
        {
            collectPostingLists(postingLists, nodeIDs);
        }

        protected void closeOnException()
        {
            FileUtils.closeQuietly(bkdInput, postingsInput, postingsSummaryInput);
        }

        protected PostingList mergePostings(PriorityQueue<PostingList.PeekablePostingList> postingLists)
        {
            final long elapsedMicros = queryExecutionTimer.stop().elapsed(TimeUnit.MICROSECONDS);

            listener.onIntersectionComplete(elapsedMicros, TimeUnit.MICROSECONDS);
            listener.postingListsHit(postingLists.size());

            if (postingLists.isEmpty())
            {
                FileUtils.closeQuietly(postingsInput, postingsSummaryInput);
                return null;
            }
            else
            {
                if (logger.isTraceEnabled())
                    logger.trace(indexComponents.logMessage("[{}] Intersection completed in {} microseconds. {} leaf and internal posting lists hit."),
                                 indexFile.path(), elapsedMicros, postingLists.size());
                return MergePostingList.merge(postingLists, () -> FileUtils.close(postingsInput, postingsSummaryInput));
            }
        }

        public void collectPostingLists(PriorityQueue<PostingList.PeekablePostingList> postingLists,
                                        Set<Integer> nodeIDs) throws IOException
        {
            context.checkpoint();

            final int nodeID = index.getNodeID();

            // if there is pre-built posting for entire subtree
            if (postingsIndex.exists(nodeID))
            {
                nodeIDs.add(nodeID);
                postingLists.add(initPostingReader(postingsIndex.getPostingsFilePointer(nodeID)).peekable());
                return;
            }

            Preconditions.checkState(!index.isLeafNode(), "Leaf node %s does not have kd-tree postings.", index.getNodeID());

            // Recurse on left sub-tree:
            index.pushLeft();
            collectPostingLists(postingLists, nodeIDs);
            index.pop();

            // Recurse on right sub-tree:
            index.pushRight();
            collectPostingLists(postingLists, nodeIDs);
            index.pop();
        }

        private PostingList initPostingReader(long offset) throws IOException
        {
            final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(postingsSummaryInput, offset);
            return new PostingsReader(postingsInput, summary, listener.postingListEventListener());
        }
    }

    /**
     * Modified copy of BKDReader#visitDocValues()
     */
    private int visitDocValues(int[] commonPrefixLengths,
                               byte[] scratchPackedValue1,
                               IndexInput in,
                               int count,
                               IntersectVisitor visitor,
                               FixedBitSet[] holder,
                               final short[] origIndex) throws IOException
    {
        readCommonPrefixes(commonPrefixLengths, scratchPackedValue1, in);

        int compressedDim = readCompressedDim(in);
        if (compressedDim == -1)
        {
            return visitRawDocValues(commonPrefixLengths, scratchPackedValue1, in, count, visitor, holder, origIndex);
        }
        else
        {
            return visitCompressedDocValues(commonPrefixLengths, scratchPackedValue1, in, count, visitor, compressedDim, holder, origIndex);
        }
    }

    /**
     * Modified copy of {@link org.apache.lucene.util.bkd.BKDReader#readCompressedDim(IndexInput)}
     */
    @SuppressWarnings("JavadocReference")
    private int readCompressedDim(IndexInput in) throws IOException
    {
        int compressedDim = in.readByte();
        if (compressedDim < -1 || compressedDim >= numDims)
        {
            throw new CorruptIndexException(String.format("Dimension should be in the range [-1, %d), but was %d.", numDims, compressedDim), in);
        }
        return compressedDim;
    }

    /**
     * Modified copy of BKDReader#visitCompressedDocValues()
     */
    private int visitCompressedDocValues(int[] commonPrefixLengths,
                                         byte[] scratchPackedValue,
                                         IndexInput in,
                                         int count,
                                         IntersectVisitor visitor,
                                         int compressedDim,
                                         FixedBitSet[] holder,
                                         final short[] origIndex) throws IOException
    {
        // the byte at `compressedByteOffset` is compressed using run-length compression,
        // other suffix bytes are stored verbatim
        final int compressedByteOffset = compressedDim * bytesPerDim + commonPrefixLengths[compressedDim];
        commonPrefixLengths[compressedDim]++;
        int i, collected = 0;

        final FixedBitSet bitSet;
        if (holder != null)
        {
            bitSet = new FixedBitSet(maxPointsInLeafNode);
        }
        else
        {
            bitSet = null;
        }

        for (i = 0; i < count; )
        {
            scratchPackedValue[compressedByteOffset] = in.readByte();
            final int runLen = Byte.toUnsignedInt(in.readByte());
            for (int j = 0; j < runLen; ++j)
            {
                for (int dim = 0; dim < numDims; dim++)
                {
                    int prefix = commonPrefixLengths[dim];
                    in.readBytes(scratchPackedValue, dim * bytesPerDim + prefix, bytesPerDim - prefix);
                }
                final int rowIDIndex = origIndex[i + j];
                if (visitor.visit(scratchPackedValue))
                {
                    if (bitSet != null) bitSet.set(rowIDIndex);
                    collected++;
                }
            }
            i += runLen;
        }
        if (i != count)
        {
            throw new CorruptIndexException(String.format("Expected %d sub-blocks but read %d.", count, i), in);
        }

        if (holder != null)
        {
            holder[0] = bitSet;
        }

        return collected;
    }

    /**
     * Modified copy of BKDReader#visitRawDocValues()
     */
    private int visitRawDocValues(int[] commonPrefixLengths,
                                  byte[] scratchPackedValue,
                                  IndexInput in,
                                  int count,
                                  IntersectVisitor visitor,
                                  FixedBitSet[] holder,
                                  final short[] origIndex) throws IOException
    {
        final FixedBitSet bitSet;
        if (holder != null)
        {
            bitSet = new FixedBitSet(maxPointsInLeafNode);
        }
        else
        {
            bitSet = null;
        }

        int collected = 0;
        for (int i = 0; i < count; ++i)
        {
            for (int dim = 0; dim < numDims; dim++)
            {
                int prefix = commonPrefixLengths[dim];
                in.readBytes(scratchPackedValue, dim * bytesPerDim + prefix, bytesPerDim - prefix);
            }
            final int rowIDIndex = origIndex[i];
            if (visitor.visit(scratchPackedValue))
            {
                if (bitSet != null) bitSet.set(rowIDIndex);

                collected++;
            }
        }
        if (holder != null)
        {
            holder[0] = bitSet;
        }
        return collected;
    }

    /**
     * Copy of BKDReader#readCommonPrefixes()
     */
    private void readCommonPrefixes(int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in) throws IOException
    {
        for (int dim = 0; dim < numDims; dim++)
        {
            int prefix = in.readVInt();
            commonPrefixLengths[dim] = prefix;
            if (prefix > 0)
            {
//                System.out.println("dim * bytesPerDim="+(dim * bytesPerDim)+" prefix="+prefix+" numDims="+numDims);
                in.readBytes(scratchPackedValue, dim * bytesPerDim, prefix);
            }
        }
    }

    private class FilteringIntersection extends Intersection
    {
        private final IntersectVisitor visitor;
        private final byte[] scratchPackedValue1;
        private final int[] commonPrefixLengths;
        private final short[] origIndex;

        // reused byte arrays for the decompression of leaf values
        private final BytesRef uncompBytes = new BytesRef(new byte[16]);
        private final BytesRef compBytes = new BytesRef(new byte[16]);

        FilteringIntersection(IndexInput bkdInput, IndexInput postingsInput, IndexInput postingsSummaryInput,
                              IndexTree index, IntersectVisitor visitor,
                              QueryEventListener.BKDIndexEventListener listener, QueryContext context)
        {
            super(bkdInput, postingsInput, postingsSummaryInput, index, listener, context);
            this.visitor = visitor;
            this.commonPrefixLengths = new int[numDims];
            this.scratchPackedValue1 = new byte[packedBytesLength];
            this.origIndex = new short[maxPointsInLeafNode];
        }

        @Override
        public void executeInternal(final PriorityQueue<PostingList.PeekablePostingList> postingLists, Set<Integer> nodeIDs) throws IOException
        {
            collectPostingLists(postingLists, minPackedValue, maxPackedValue, nodeIDs);
        }

        public void collectPostingLists(PriorityQueue<PostingList.PeekablePostingList> postingLists,
                                        byte[] cellMinPacked,
                                        byte[] cellMaxPacked,
                                        Set<Integer> nodeIDs) throws IOException
        {
            context.checkpoint();

            final Relation r = visitor.compare(cellMinPacked, cellMaxPacked);

            if (r == Relation.CELL_OUTSIDE_QUERY)
            {
                // This cell is fully outside of the query shape: stop recursing
                return;
            }

            if (r == Relation.CELL_INSIDE_QUERY)
            {
                // This cell is fully inside of the query shape: recursively add all points in this cell without filtering
                super.collectPostingLists(postingLists, nodeIDs);
                return;
            }

            if (index.isLeafNode())
            {
                if (index.nodeExists())
                {
                    System.out.println("regular leafNodeID="+index.getNodeID());
                    nodeIDs.add(index.getNodeID());
                    filterLeaf(postingLists);
                }
                return;
            }

            visitNode(postingLists, cellMinPacked, cellMaxPacked, nodeIDs);
        }

        @SuppressWarnings("resource")
        void filterLeaf(PriorityQueue<PostingList.PeekablePostingList> postingLists) throws IOException
        {
            bkdInput.seek(index.getLeafBlockFP());

            final int count = bkdInput.readVInt();

            // loading doc ids occurred here prior

            final FixedBitSet[] holder = new FixedBitSet[1];

            final int orderMapLength = bkdInput.readVInt();

            final long orderMapPointer = bkdInput.getFilePointer();

            final SeekingRandomAccessInput randoInput = new SeekingRandomAccessInput(bkdInput);
            for (int x = 0; x < count; x++)
            {
                origIndex[x] = (short) LeafOrderMap.getValue(randoInput, orderMapPointer, x, leafOrderMapReader);
            }

            // seek beyond the ordermap
            bkdInput.seek(orderMapPointer + orderMapLength);

            IndexInput leafInput = bkdInput;

            if (compressor != null)
            {
                // This should not throw WouldBlockException, even though we're on a TPC thread, because the
                // secret key used by the underlying encryptor should be loaded at reader construction time.
                leafInput = CryptoUtils.uncompress(bkdInput, compressor, compBytes, uncompBytes);
            }

            visitDocValues(commonPrefixLengths, scratchPackedValue1, leafInput, count, visitor, holder, origIndex);

            final int nodeID = index.getNodeID();

            if (postingsIndex.exists(nodeID) && holder[0].cardinality() > 0)
            {
                final long pointer = postingsIndex.getPostingsFilePointer(nodeID);
                postingLists.add(initFilteringPostingReader(pointer, holder[0]).peekable());
            }
        }

        void visitNode(PriorityQueue<PostingList.PeekablePostingList> postingLists,
                       byte[] cellMinPacked,
                       byte[] cellMaxPacked,
                       Set<Integer> nodeIDs) throws IOException
        {
            int splitDim = index.getSplitDim();
            assert splitDim >= 0 : "splitDim=" + splitDim;
            assert splitDim < numDims;

            byte[] splitPackedValue = index.getSplitPackedValue();
            BytesRef splitDimValue = index.getSplitDimValue();
            assert splitDimValue.length == bytesPerDim;

            // make sure cellMin <= splitValue <= cellMax:
            assert FutureArrays.compareUnsigned(cellMinPacked, splitDim * bytesPerDim, splitDim * bytesPerDim + bytesPerDim, splitDimValue.bytes, splitDimValue.offset, splitDimValue.offset + bytesPerDim) <= 0 : "bytesPerDim=" + bytesPerDim + " splitDim=" + splitDim + " numDims=" + numDims;
            assert FutureArrays.compareUnsigned(cellMaxPacked, splitDim * bytesPerDim, splitDim * bytesPerDim + bytesPerDim, splitDimValue.bytes, splitDimValue.offset, splitDimValue.offset + bytesPerDim) >= 0 : "bytesPerDim=" + bytesPerDim + " splitDim=" + splitDim + " numDims=" + numDims;

            // Recurse on left sub-tree:
            System.arraycopy(cellMaxPacked, 0, splitPackedValue, 0, packedBytesLength);
            System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, splitDim * bytesPerDim, bytesPerDim);

            index.pushLeft();
            collectPostingLists(postingLists, cellMinPacked, splitPackedValue, nodeIDs);
            index.pop();

            // Restore the split dim value since it may have been overwritten while recursing:
            System.arraycopy(splitPackedValue, splitDim * bytesPerDim, splitDimValue.bytes, splitDimValue.offset, bytesPerDim);
            // Recurse on right sub-tree:
            System.arraycopy(cellMinPacked, 0, splitPackedValue, 0, packedBytesLength);
            System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, splitDim * bytesPerDim, bytesPerDim);
            index.pushRight();
            collectPostingLists(postingLists, splitPackedValue, cellMaxPacked, nodeIDs);
            index.pop();
        }

        private PostingList initFilteringPostingReader(long offset, FixedBitSet filter) throws IOException
        {
            final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(postingsSummaryInput, offset);
            return initFilteringPostingReader(filter, summary);
        }

        @SuppressWarnings("resource")
        private PostingList initFilteringPostingReader(FixedBitSet filter, PostingsReader.BlocksSummary header) throws IOException
        {
            PostingsReader postingsReader = new PostingsReader(postingsInput, header, listener.postingListEventListener());
            return new FilteringPostingList(filter, postingsReader);
        }
    }

    public int getNumDimensions()
    {
        return numDims;
    }

    public int getBytesPerDimension()
    {
        return bytesPerDim;
    }

    public long getPointCount()
    {
        return pointCount;
    }

    /**
     * We recurse the BKD tree, using a provided instance of this to guide the recursion.
     */
    public interface IntersectVisitor
    {
        /**
         * Called for all values in a leaf cell that crosses the query.  The consumer
         * should scrutinize the packedValue to decide whether to accept it.  In the 1D case,
         * values are visited in increasing order, and in the case of ties, in increasing order
         * by segment row ID.
         */
        boolean visit(byte[] packedValue);

        /**
         * Called for non-leaf cells to test how the cell relates to the query, to
         * determine how to further recurse down the tree.
         */
        Relation compare(byte[] minPackedValue, byte[] maxPackedValue);
    }
}
