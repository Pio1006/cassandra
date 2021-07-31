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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntLongHashMap;
import jnr.ffi.annotations.In;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.disk.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.PackedLongValues;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Writes auxiliary posting lists for bkd tree nodes. If a node has a posting list attached, it will contain every row
 * id
 * from all leaves reachable from that node.
 *
 * Writer is stateful, because it needs to collect data from bkd index data structure first to find set of eligible
 * nodes and leaf nodes reachable from them.
 *
 * This is an optimised writer for 1-dim points, where we know that leaf blocks are written in value order (in this
 * order we pass them to the {@link BKDWriter}). That allows us to skip reading the leaves, instead just order leaf
 * blocks by their offset in the index file, and correlate them with buffered posting lists. We can't make this
 * assumption for multi-dim case.
 */
public class MultiLevelPostingsWriter
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    //private final TreeMap<Long, Integer> leafFPToNodeID = new TreeMap<>(Long::compareTo);
    private final Multimap<Integer, Integer> nodeToChildLeaves = HashMultimap.create();

    final TreeSet<Integer> leafNodeIDs = new TreeSet<>();

    private final IndexWriterConfig config;
    final TreeMap<Integer,Integer> nodeIDToLeafOrdinal;
    // private final IndexComponents components;
    final IntLongHashMap nodeIDPostingsFP;
    final IndexInput leafPostingsInput;
    final int numLeaves;
    int numNonLeafPostings = 0;
    int numLeafPostings = 0;
    final RangeSet<Integer> multiBlockLeafRanges;

    public MultiLevelPostingsWriter(IndexInput leafPostingsInput,
                                    IndexWriterConfig config,
                                    IntLongHashMap nodeIDPostingsFP,
                                    int numLeaves,
                                    TreeMap<Integer,Integer> nodeIDToLeafOrdinal,
                                    final RangeSet<Integer> multiBlockLeafRanges)
    {
        this.leafPostingsInput = leafPostingsInput;
        this.config = config;
        this.nodeIDPostingsFP = nodeIDPostingsFP;
        this.numLeaves = numLeaves;
        this.nodeIDToLeafOrdinal = nodeIDToLeafOrdinal;
        this.multiBlockLeafRanges = multiBlockLeafRanges;
    }

    @SuppressWarnings("resource")
    // TODO: possibly writing lower level leaf nodes twice?
    public long finish(IndexOutput out) throws IOException
    {
        traverse(new BinaryTreeIndex(numLeaves),
                 new IntArrayList());

//        checkState(postings.size() == leafFPToNodeID.size(),
//                   "Expected equal number of postings lists (%s) and leaf offsets (%s).",
//                   postings.size(), leafFPToNodeID.size());

        final PostingsWriter postingsWriter = new PostingsWriter(out);

        final List<Integer> internalNodeIDs =
        nodeToChildLeaves.keySet()
                         .stream()
                         .filter(i -> nodeToChildLeaves.get(i).size() >= config.getBkdPostingsMinLeaves())
                         .collect(Collectors.toList());

        final long startFP = out.getFilePointer();
        final Stopwatch flushTime = Stopwatch.createStarted();
        final TreeMap<Integer, Long> nodeIDToPostingsFilePointer = new TreeMap<>();
        for (int nodeID : Iterables.concat(internalNodeIDs, leafNodeIDs))
        {
            Collection<Integer> leafNodeIDs = nodeToChildLeaves.get(nodeID);

            final Integer leafOrdinal = nodeIDToLeafOrdinal.get(nodeID);

            assert leafOrdinal != null;

            final TreeMap<Integer,Integer> leafToNodeID;

            if (leafNodeIDs.size() == 0)
            {
                assert this.leafNodeIDs.contains(nodeID);

                leafNodeIDs = Collections.singletonList(nodeID);
                numLeafPostings++;
                leafToNodeID = null;
            }
            else
            {
                // skip leaf nodes they're not overlapping with multi-block postings
                // if there are overlapping same value multi-block postings, add the multi-block postings
                // file pointers, and remove the multi-block node ids from leafNodeIDs
                // so the same value multi-block postings aren't added to the aggregated node posting list
                leafToNodeID = new TreeMap<>();
                for (final int leafNodeID : leafNodeIDs)
                {
                    final int leaf = nodeIDToLeafOrdinal.get(leafNodeID);
                    leafToNodeID.put(leaf, leafNodeID);
                }

                final int minLeaf = leafToNodeID.firstKey();
                final int maxLeaf = leafToNodeID.lastKey();

                // if there are multi-block ranges then remove their node ids from the ultimate posting list
                final Range<Integer> multiBlockMin = multiBlockLeafRanges.rangeContaining(minLeaf);
                final Range<Integer> multiBlockMax = multiBlockLeafRanges.rangeContaining(maxLeaf);

                if (multiBlockMin != null)
                {
                    final int startLeaf = multiBlockMin.lowerEndpoint();
                    final int endLeaf = multiBlockMin.upperEndpoint();

                    // remove leaf node's that are in the multi-block posting list
                    for (int leaf = startLeaf; leaf <= endLeaf; leaf++)
                    {
                        Integer leafRemoved = leafToNodeID.remove(leaf);
                        if (leafRemoved != null)
                           System.out.println("multiBlockMin leafRemoved leaf="+leaf);
                    }
                }

                if (multiBlockMax != null)
                {
                    final int startLeaf = multiBlockMax.lowerEndpoint();
                    final int endLeaf = multiBlockMax.upperEndpoint();

                    for (int leaf = startLeaf; leaf <= endLeaf; leaf++)
                    {
                        Integer leafRemoved = leafToNodeID.remove(leaf);
                        if (leafRemoved != null)
                            System.out.println("multiBlockMax leafRemoved leaf="+leaf);
                    }
                }

                numNonLeafPostings++;
            }

            final PriorityQueue<PostingList.PeekablePostingList> postingLists = new PriorityQueue<>(100, Comparator.comparingLong(PostingList.PeekablePostingList::peek));

            Collection<Integer> reducedNodeIDs = leafNodeIDs;
            if (leafToNodeID != null)
            {
                reducedNodeIDs = leafToNodeID.values();
            }

            for (final Integer leafNodeID : reducedNodeIDs)
            {
                // assert nodeIDPostingsFP.containsKey(leafNodeID);

                if (nodeIDPostingsFP.containsKey(leafNodeID))
                {
                    final long postingsFP = nodeIDPostingsFP.get(leafNodeID);
                    final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(leafPostingsInput, postingsFP);
                    final PostingsReader reader = new PostingsReader(leafPostingsInput, summary, QueryEventListener.PostingListEventListener.NO_OP);
                    postingLists.add(reader.peekable());
                }
            }

            if (postingLists.size() > 0)
            {
                final PostingList mergedPostingList = MergePostingList.merge(postingLists);
                final long postingFilePosition = postingsWriter.write(mergedPostingList);
                // During compaction we could end up with an empty postings due to deletions.
                // The writer will return a fp of -1 if no postings were written.
                if (postingFilePosition >= 0)
                {
                    nodeIDToPostingsFilePointer.put(nodeID, postingFilePosition);
                }
            }
        }
        flushTime.stop();
//        logger.debug(components.logMessage("Flushed {} of posting lists for kd-tree nodes in {} ms."),
//                     FBUtilities.prettyPrintMemory(out.getFilePointer() - startFP),
//                     flushTime.elapsed(TimeUnit.MILLISECONDS));


        final long indexFilePointer = out.getFilePointer();
        writeMap(nodeIDToPostingsFilePointer, out);
        postingsWriter.complete();
        return indexFilePointer;
    }

    private void onLeaf(int leafNodeID, IntArrayList pathToRoot)
    {
        checkArgument(!pathToRoot.containsInt(leafNodeID));
        checkArgument(pathToRoot.isEmpty() || leafNodeID > pathToRoot.get(pathToRoot.size() - 1));

        leafNodeIDs.add(leafNodeID);

        for (int i = 0; i < pathToRoot.size(); i++)
        {
            final int level = i + 1;
            if (isLevelEligibleForPostingList(level))
            {
                final int nodeID = pathToRoot.get(i);
                nodeToChildLeaves.put(nodeID, leafNodeID);
            }
        }
    }

    private void traverse(BinaryTreeIndex tree,
                          IntArrayList pathToRoot)
    {
        if (tree.isLeafNode())
        {
            // In the unbalanced case it's possible the left most node only has one child:
            if (tree.nodeExists())
            {
                onLeaf(tree.getNodeID(), pathToRoot);
            }
        }
        else
        {
            final int nodeID = tree.getNodeID();
            final IntArrayList currentPath = new IntArrayList();
            currentPath.addAll(pathToRoot);
            currentPath.add(nodeID);

            tree.pushLeft();
            traverse(tree, currentPath);
            tree.pop();

            tree.pushRight();
            traverse(tree, currentPath);
            tree.pop();
        }
    }

    private boolean isLevelEligibleForPostingList(int level)
    {
        return level > 1 && level % config.getBkdPostingsSkip() == 0;
    }

    private void writeMap(Map<Integer, Long> map, IndexOutput out) throws IOException
    {
        out.writeVInt(map.size());

        for (Map.Entry<Integer, Long> e : map.entrySet())
        {
            out.writeVInt(e.getKey());
            out.writeVLong(e.getValue());
        }
    }
}
