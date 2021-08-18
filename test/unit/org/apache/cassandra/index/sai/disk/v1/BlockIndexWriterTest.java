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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import static org.apache.cassandra.index.sai.disk.v1.BKDReaderTest.buildQuery;
import static org.apache.cassandra.index.sai.metrics.QueryEventListeners.NO_OP_BKD_LISTENER;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class BlockIndexWriterTest extends NdiRandomizedTest
{
    @Test
    public void test() throws Exception
    {
        List<Pair<ByteComparable, IntArrayList>> list = new ArrayList();
        list.add(add("aaabbb", new int[] {0, 2})); // 2
        list.add(add("aaabbbbbb", new int[] {1, 3})); // 2
        list.add(add("aaabbbccccc", new int[] {4, 5, 6})); // 3
        list.add(add("zzzzzzzggg", new int[] {10, 11, 12})); // 3

        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexOutput out = dir.createOutput("file", IOContext.DEFAULT);

        IndexComponents comps = newIndexComponents();

        IndexOutputWriter indexOut = comps.createOutput(comps.kdTree);
        IndexOutputWriter postingsOut = comps.createOutput(comps.kdTreePostingLists);

//        BlockIndexWriter prefixBytesWriter = new BlockIndexWriter(out, indexOut, postingsOut);
//
//        TermsIterator terms = new MemtableTermsIterator(null,
//                                                        null,
//                                                        list.iterator());
//
//        int pointCount = 0;
//        while (terms.hasNext())
//        {
//            ByteComparable term = terms.next();
//            PostingList postings = terms.postings();
//            while (true)
//            {
//                long rowID = postings.nextPosting();
//                if (rowID == PostingList.END_OF_STREAM) break;
//                prefixBytesWriter.add(term, rowID);
//                pointCount++;
//            }
//        }

//        BlockIndexWriter.BlockIndexMeta meta = prefixBytesWriter.finish();
//
//        postingsOut.close();
//
//        try (IndexInput input = dir.openInput("file", IOContext.DEFAULT);
//             IndexInput input2 = dir.openInput("file", IOContext.DEFAULT))
//        {
//            FileHandle indexFile = comps.createFileHandle(comps.kdTree);
//            BlockIndexReader reader = new BlockIndexReader(input,
//                                                           input2,
//                                                           indexFile,
//                                                           meta);

//            reader.traverseForFilePointers(fixedLength(new BytesRef("aaa")),
//                                           fixedLength(new BytesRef("aaabbbcczzzz")));

//            reader.traverse(fixedLength(new BytesRef("aaa")),
//                            fixedLength(new BytesRef("aaabbbcczzzz")));

//            BytesRef foundTerm = reader.seekTo(new BytesRef("aaabbbccddddd"));
//            System.out.println("foundTerm="+foundTerm.utf8ToString());

//            BytesRef term3 = reader.seekTo(3);
//            System.out.println("term3="+term3.utf8ToString());
//
//            BytesRef term1 = reader.seekTo(1);
//            System.out.println("term1="+term1.utf8ToString());
//
//            BytesRef term6 = reader.seekTo(6);
//            System.out.println("term6="+term6.utf8ToString());
//
//            BytesRef term8 = reader.seekTo(8);
//            System.out.println("term8="+term8.utf8ToString());
//
//            List<String> results = new ArrayList<>();
//            for (int x=0; x < pointCount; x++)
//            {
//                BytesRef term = reader.seekTo(x);
//                results.add(term.utf8ToString());
//                System.out.println("x="+x+" term=" + term.utf8ToString());
//            }
//            System.out.println("results="+results);
        }
    //}

    @Test
    public void testRanges() throws Exception
    {
        TreeRangeSet<Integer> set = TreeRangeSet.create();
        set.add(Range.closed(0, 10));
        set.add(Range.open(50, 60));

        boolean contains = set.contains(10);
        System.out.println("contains="+contains);

        boolean encloses = set.encloses(Range.open(0, 10));
        System.out.println("encloses="+encloses);

        boolean encloses2 = set.encloses(Range.open(4, 15));

        System.out.println("encloses2="+encloses2);
    }

    @Test
    public void testSameTerms() throws Exception
    {
        List<Pair<ByteComparable, IntArrayList>> list = new ArrayList();
        list.add(add("aaa", new int[] {100})); // 0
        list.add(add("aaabbb", new int[] {0, 1})); // 0
        list.add(add("aaabbb", new int[] {2, 3, 4})); // 1
        list.add(add("cccc", new int[] {10, 11})); // 2
        list.add(add("cccc", new int[] {15})); // 3
        list.add(add("gggaaaddd", new int[] {200, 201, 203})); // 3, 4
        list.add(add("gggzzzz", new int[] {500, 501, 502, 503, 504, 505})); // 5, 6
        list.add(add("qqqqqaaaaa", new int[] {400, 405, 409})); // 3, 4
        list.add(add("zzzzz", new int[] {700, 780, 782, 790, 794, 799})); // 6, 7
//        list.add(add("zzzzz", new int[] {16, 17, 18})); // 8, 9
//        list.add(add("zzzzzzzzzz", new int[] {20, 21, 24, 29, 30})); // 8, 9

        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexOutput out = dir.createOutput("file", IOContext.DEFAULT);
        IndexOutput ordermapout = dir.createOutput("ordermap", IOContext.DEFAULT);

        IndexComponents comps = newIndexComponents();
        IndexOutputWriter indexOut = comps.createOutput(comps.kdTree);
        //IndexOutput postingsOut = dir.createOutput("postings", IOContext.DEFAULT);//comps.createOutput(comps.kdTreePostingLists);

        BlockIndexWriter prefixBytesWriter = new BlockIndexWriter(out, indexOut, ordermapout, dir);

        TermsIterator terms = new MemtableTermsIterator(null,
                                                        null,
                                                        list.iterator());

        int pointCount = 0;
        while (terms.hasNext())
        {
            ByteComparable term = terms.next();
            PostingList postings = terms.postings();
            while (true)
            {
                long rowID = postings.nextPosting();
                if (rowID == PostingList.END_OF_STREAM) break;
                prefixBytesWriter.add(term, rowID);
                pointCount++;
            }
        }

        BlockIndexWriter.BlockIndexMeta meta = prefixBytesWriter.finish();

        //postingsOut.close();

        try (IndexInput input = dir.openInput("file", IOContext.DEFAULT);
             IndexInput input2 = dir.openInput("file", IOContext.DEFAULT);
             IndexInput ordermapInput = dir.openInput("ordermap", IOContext.DEFAULT);
             IndexInput postingsInput = dir.openInput("leafpostings", IOContext.DEFAULT))
        {
            FileHandle indexFile = comps.createFileHandle(comps.kdTree);
            BlockIndexReader reader = new BlockIndexReader(input,
                                                           input2,
                                                           indexFile,
                                                           ordermapInput,
                                                           postingsInput,
                                                           postingsInput,
                                                           meta);

//            PostingList postings = reader.traverse(ByteComparable.fixedLength("aa".getBytes(StandardCharsets.UTF_8)),
//                                                   ByteComparable.fixedLength("ab".getBytes(StandardCharsets.UTF_8)));

//            PostingList postings = reader.traverse(ByteComparable.fixedLength("aaabbb".getBytes(StandardCharsets.UTF_8)),
//                                                   true,
//                                                   ByteComparable.fixedLength("gggaaaddd".getBytes(StandardCharsets.UTF_8)),
//                                                   true);

            PostingList postings = reader.traverse(null,
                                                   true,
                                                   ByteComparable.fixedLength("cccc".getBytes(StandardCharsets.UTF_8)),
                                                   false);

            while (true)
            {
                final long rowID = postings.nextPosting();
                if (rowID == PostingList.END_OF_STREAM) break;
                System.out.println("testSameTerms rowid=" + rowID);
            }
        }

            // maxNodeID,
//            PostingList leafPostings = reader.filterFirstLeaf(minNodeID,
//                                                              new BytesRef("zzzzz"),
//                                                              false);
//
//            while (true)
//            {
//                final long rowID = leafPostings.nextPosting();
//                if (rowID == PostingList.END_OF_STREAM) break;
//                System.out.println("rowid="+rowID);
//            }

//            PostingList leafPostings = reader.filterFirstLeaf(minNodeID,
//                                                              new BytesRef("zzzz"),
//                                                              false);
//
//            while (true)
//            {
//                final long rowID = leafPostings.nextPosting();
//                if (rowID == PostingList.END_OF_STREAM) break;
//                System.out.println("rowid="+rowID);
//            }

//            reader.traverseForFilePointers(fixedLength(new BytesRef("aaa")),
//                                           fixedLength(new BytesRef("aaabbbcczzzz")));

//            reader.traverse(fixedLength(new BytesRef("aaa")),
//                            fixedLength(new BytesRef("aaabbbcczzzz")));

//            BytesRef foundTerm = reader.seekTo(new BytesRef("aaabbbccddddd"));
//            System.out.println("foundTerm="+foundTerm.utf8ToString());

//            BytesRef term3 = reader.seekTo(3);
//            System.out.println("term3="+term3.utf8ToString());
//
//            BytesRef term1 = reader.seekTo(1);
//            System.out.println("term1="+term1.utf8ToString());
//
//            BytesRef term6 = reader.seekTo(6);
//            System.out.println("term6="+term6.utf8ToString());
//
//            BytesRef term8 = reader.seekTo(8);
//            System.out.println("term8="+term8.utf8ToString());
//
//            List<String> results = new ArrayList<>();
//            for (int x=0; x < pointCount; x++)
//            {
//                BytesRef term = reader.seekTo(x);
//                results.add(term.utf8ToString());
//                System.out.println("x="+x+" term=" + term.utf8ToString());
//            }
//            System.out.println("results="+results);
       // }
    }

    @Test
    public void randomTest() throws Exception
    {
        for (int x = 0; x < 100; x++)
        {
            doRandomTest();
        }
    }

    public void doRandomTest() throws Exception
    {
        List<Pair<ByteComparable, IntArrayList>> list = new ArrayList();
        int numValues = nextInt(5, 200);
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, Integer.BYTES);

        int maxRowID = -1;
        int totalRows = 0;
        for (int x = 0; x < numValues; x++)
        {
            byte[] scratch = new byte[4];
            NumericUtils.intToSortableBytes(x, scratch, 0);

            int numRows = nextInt(1, 20);
            int rowID = nextInt(100);
            IntArrayList postings = new IntArrayList();
            for (int i = 0; i < numRows; i++)
            {
                maxRowID = Math.max(maxRowID, rowID);
                postings.add(rowID);
                totalRows++;
                buffer.addPackedValue(rowID, new BytesRef(scratch));
                rowID += nextInt(1, 100);
            }
            System.out.println("term="+x+" postings="+postings);
            list.add(Pair.create(ByteComparable.fixedLength(scratch), postings));
        }

        int startIdx = nextInt(0, numValues - 2);
        ByteComparable start = list.get(startIdx).left;

        int queryMin = NumericUtils.sortableBytesToInt(ByteSourceInverse.readBytes(start.asComparableBytes(ByteComparable.Version.OSS41)), 0);

        int endIdx = nextInt(startIdx + 1, numValues - 1);
        ByteComparable end = list.get(endIdx).left;

        int queryMax = NumericUtils.sortableBytesToInt(ByteSourceInverse.readBytes(end.asComparableBytes(ByteComparable.Version.OSS41)), 0);

        System.out.println("queryMin="+queryMin+" queryMax="+queryMax+" totalRows="+totalRows+" maxRowID="+maxRowID);

        final IndexComponents indexComponents = newIndexComponents();

        final BKDReader bkdReader = finishAndOpenReaderOneDim(2,
                                                              buffer,
                                                              indexComponents,
                                                              maxRowID + 1,
                                                              totalRows);

        final PostingList kdtreePostings = bkdReader.intersect(buildQuery(queryMin, queryMax), NO_OP_BKD_LISTENER, new QueryContext());
        IntArrayList kdtreePostingList = collect(kdtreePostings);
        kdtreePostings.close();
        bkdReader.close();


        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexOutput out = dir.createOutput("file", IOContext.DEFAULT);
        IndexOutput ordermapout = dir.createOutput("ordermap", IOContext.DEFAULT);

        IndexComponents comps = newIndexComponents();
        IndexOutputWriter indexOut = comps.createOutput(comps.kdTree);

        BlockIndexWriter prefixBytesWriter = new BlockIndexWriter(out, indexOut, ordermapout, dir);

        TermsIterator terms = new MemtableTermsIterator(null,
                                                        null,
                                                        list.iterator());

        int pointCount = 0;
        while (terms.hasNext())
        {
            ByteComparable term = terms.next();
            PostingList postings = terms.postings();
            while (true)
            {
                long rowID = postings.nextPosting();
                if (rowID == PostingList.END_OF_STREAM) break;
                prefixBytesWriter.add(term, rowID);
                pointCount++;
            }
        }

        BlockIndexWriter.BlockIndexMeta meta = prefixBytesWriter.finish();

        ordermapout.close();
        out.close();
        indexOut.close();

        try (IndexInput input = dir.openInput("file", IOContext.DEFAULT);
             IndexInput input2 = dir.openInput("file", IOContext.DEFAULT);
             IndexInput ordermapInput = dir.openInput("ordermap", IOContext.DEFAULT);
             IndexInput postingsInput = dir.openInput("leafpostings", IOContext.DEFAULT);
             IndexInput bigpostingsInput = dir.openInput("bigpostings", IOContext.DEFAULT))
        //
        {
            FileHandle indexFile = comps.createFileHandle(comps.kdTree);
            BlockIndexReader reader = new BlockIndexReader(input,
                                                           input2,
                                                           indexFile,
                                                           ordermapInput,
                                                           postingsInput,
                                                           bigpostingsInput,
                                                           meta);

            PostingList postings = reader.traverse(start, end);
            IntArrayList results2 = collect(postings);

            System.out.println("kdtreePostingList="+kdtreePostingList);
            System.out.println("results2="+results2);

            assertEquals(kdtreePostingList, results2);
        }
    }

    private IntArrayList collect(PostingList postings) throws IOException
    {
        IntArrayList list = new IntArrayList();
        if (postings == null) return list;
        while (true)
        {
            long rowid = postings.nextPosting();
            if (rowid == PostingList.END_OF_STREAM) break;
            if (list.size() > 0)
            {
                if (list.get(list.size() - 1) == rowid)
                {
                    continue;
                }
            }
            list.add((int) rowid);
        }
        return list;
    }

    private BKDReader finishAndOpenReaderOneDim(int maxPointsPerLeaf,
                                                BKDTreeRamBuffer buffer,
                                                IndexComponents indexComponents,
                                                int maxRowID,
                                                int totalRows) throws IOException
    {
        final NumericIndexWriter writer = new NumericIndexWriter(indexComponents,
                                                                 maxPointsPerLeaf,
                                                                 Integer.BYTES,
                                                                 maxRowID,
                                                                 totalRows,
                                                                 new IndexWriterConfig("test", 2, 8),
                                                                 false);

        final SegmentMetadata.ComponentMetadataMap metadata = writer.writeAll(buffer.asPointValues());
        final long bkdPosition = metadata.get(IndexComponents.NDIType.KD_TREE).root;
        assertThat(bkdPosition, is(greaterThan(0L)));
        final long postingsPosition = metadata.get(IndexComponents.NDIType.KD_TREE_POSTING_LISTS).root;
        assertThat(postingsPosition, is(greaterThan(0L)));

        FileHandle kdtree = indexComponents.createFileHandle(indexComponents.kdTree);
        FileHandle kdtreePostings = indexComponents.createFileHandle(indexComponents.kdTreePostingLists);
        return new BKDReader(indexComponents,
                             kdtree,
                             bkdPosition,
                             kdtreePostings,
                             postingsPosition);
    }

    public static Pair<ByteComparable, IntArrayList> add(String term, int[] array)
    {
        IntArrayList list = new IntArrayList();
        list.add(array, 0, array.length);
        return Pair.create(ByteComparable.fixedLength(UTF8Type.instance.decompose(term)), list);
    }
}
