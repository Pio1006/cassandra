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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.agrona.collections.IntArrayList;

public class UpperPostingsWriter
{
    final BlockIndexReader reader;
    private final Multimap<Integer, Integer> nodeToChildLeaves = HashMultimap.create();

    public UpperPostingsWriter(BlockIndexReader reader)
    {
        this.reader = reader;
    }

    public void write() throws IOException
    {
        BinaryTreeIndex index = reader.binaryTreeIndex();
        IntArrayList pathToRoot = new IntArrayList();
        traverse(index, pathToRoot);


    }

    private void traverse(BinaryTreeIndex index,
                          IntArrayList pathToRoot)
    {
        if (index.isLeafNode())
        {
            // In the unbalanced case it's possible the left most node only has one child:
            if (index.nodeExists())
            {
                // callback.onLeaf(index.getNodeID(), index.getLeafBlockFP(), pathToRoot);

                int leafNodeID = index.getNodeID();

                for (int i = 0; i < pathToRoot.size(); i++)
                {
                    final int level = i + 1;
//                    if (isLevelEligibleForPostingList(level))
//                    {
                        final int nodeID = pathToRoot.get(i);
                        nodeToChildLeaves.put(nodeID, leafNodeID);
                    //}
                }
            }
        }
        else
        {
            final int nodeID = index.getNodeID();
            final IntArrayList currentPath = new IntArrayList();
            currentPath.addAll(pathToRoot);
            currentPath.add(nodeID);

            index.pushLeft();
            traverse(index, currentPath);
            index.pop();

            index.pushRight();
            traverse(index, currentPath);
            index.pop();
        }
    }
}
