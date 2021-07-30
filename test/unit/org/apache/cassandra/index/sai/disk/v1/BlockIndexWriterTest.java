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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;

import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;


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
        //list.add(add("aaa", new int[] {5, 6})); // 0
        list.add(add("aaabbb", new int[] {0, 1})); // 0
        list.add(add("aaabbb", new int[] {2, 3})); // 1
        list.add(add("cccc", new int[] {4, 5})); // 2
        list.add(add("cccc", new int[] {6})); // 3
        list.add(add("gggaaaddd", new int[] {7, 8, 9})); // 3, 4
        list.add(add("gggzzzz", new int[] {10, 11, 12})); // 5, 6
        list.add(add("zzzzz", new int[] {13, 14, 15})); // 6, 7
        list.add(add("zzzzz", new int[] {16, 17, 18})); // 8, 9
        list.add(add("zzzzzzzzzz", new int[] {20, 21})); // 8, 9

        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexOutput out = dir.createOutput("file", IOContext.DEFAULT);
        IndexOutput ordermapout = dir.createOutput("ordermap", IOContext.DEFAULT);

        IndexComponents comps = newIndexComponents();

        IndexOutputWriter indexOut = comps.createOutput(comps.kdTree);
        IndexOutput postingsOut = dir.createOutput("postings", IOContext.DEFAULT);//comps.createOutput(comps.kdTreePostingLists);

        BlockIndexWriter prefixBytesWriter = new BlockIndexWriter(out, indexOut, postingsOut, ordermapout);

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

        postingsOut.close();

        try (IndexInput input = dir.openInput("file", IOContext.DEFAULT);
             IndexInput input2 = dir.openInput("file", IOContext.DEFAULT);
             IndexInput ordermapInput = dir.openInput("ordermap", IOContext.DEFAULT);
             IndexInput postingsInput = dir.openInput("postings", IOContext.DEFAULT))
        {
            FileHandle indexFile = comps.createFileHandle(comps.kdTree);
            BlockIndexReader reader = new BlockIndexReader(input,
                                                           input2,
                                                           indexFile,
                                                           ordermapInput,
                                                           postingsInput,
                                                           meta);

//            SortedSet<Integer> nodeIDs2 = reader.traverseForNodeIDs(ByteComparable.fixedLength("zzzzz".getBytes(StandardCharsets.UTF_8)),
//                                                                    null);
//            System.out.println("nodeIDs2=" + nodeIDs2);
//
////            Set<Integer> nodeIDs = reader.traverseIndex(3, 4);
////            System.out.println("nodeIDs=" + nodeIDs);
//
//            List<Pair<Integer,Integer>> nodeIDToLeafOrd = new ArrayList<>();
//
//            for (int nodeID : nodeIDs2)
//            {
//                long leafOrdinal = reader.nodeIDToLeaf.get(nodeID);
//                Long postingsFP = null;
//                if (meta.nodeIDPostingsFP.containsKey(nodeID))
//                {
//                    postingsFP = meta.nodeIDPostingsFP.get(nodeID);
//                }
//
//                if (postingsFP != null)
//                    nodeIDToLeafOrd.add(Pair.create(nodeID, (int) leafOrdinal));
//
//                System.out.println("nodeID=" + nodeID + " leafOrdinal=" + leafOrdinal + " postingsFP=" + postingsFP);
//            }
//
//            Collections.sort(nodeIDToLeafOrd, (o1, o2) -> Integer.compare(o1.right, o2.right));
//            //int minNodeID = nodeIDToLeafOrd.get(0).left;
//            int maxNodeID = nodeIDToLeafOrd.get(nodeIDToLeafOrd.size() - 1).left;
//
//            int minNodeID = nodeIDToLeafOrd.get(0).left;

            PostingList postings = reader.traverse(ByteComparable.fixedLength("gggaaaddd".getBytes(StandardCharsets.UTF_8)),
                                                   ByteComparable.fixedLength("gggzzzz".getBytes(StandardCharsets.UTF_8)));
            while (true)
            {
                final long rowID = postings.nextPosting();
                if (rowID == PostingList.END_OF_STREAM) break;
                System.out.println("rowid="+rowID);
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
        }
    }

    public static Pair<ByteComparable, IntArrayList> add(String term, int[] array)
    {
        IntArrayList list = new IntArrayList();
        list.add(array, 0, array.length);
        return Pair.create(ByteComparable.fixedLength(UTF8Type.instance.decompose(term)), list);
    }
}
