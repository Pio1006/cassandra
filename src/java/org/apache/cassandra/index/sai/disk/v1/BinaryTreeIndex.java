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


public class BinaryTreeIndex
{
    final int leafNodeOffset;
    protected int nodeID;
    // level is 1-based so that we can do level-1 w/o checking each time:
    protected int level;

    public BinaryTreeIndex(int leafNodeOffset)
    {
        this.leafNodeOffset = leafNodeOffset;
        nodeID = 1;
        level = 1;
    }

//    public Traverser traverse(int minNodeID, int maxNodeID)
//    {
//        return new Traverser(this, minNodeID, maxNodeID);
//    }
//
//    public static class Traverser
//    {
//        public final BinaryTreeIndex tree;
//        public final int minNodeID;
//        public final int maxNodeID;
//
//        public Traverser(BinaryTreeIndex tree, int minNodeID, int maxNodeID)
//        {
//            this.tree = tree;
//            this.minNodeID = minNodeID;
//            this.maxNodeID = maxNodeID;
//        }
//
//        public void traverse()
//        {
//            final int nodeID = tree.getNodeID();
//
//            if (nodeID > maxNodeID || nodeID < minNodeID)
//            {
//                return;
//            }
//
//            if (tree.isLeafNode())
//            {
//                // In the unbalanced case it's possible the left most node only has one child:
//                if (tree.nodeExists())
//                {
//
//                    //callback.onLeaf(index.getNodeID(), index.getLeafBlockFP(), pathToRoot);
//                }
//            }
//            else
//            {
////                final IntArrayList currentPath = new IntArrayList();
////                currentPath.addAll(pathToRoot);
////                currentPath.add(nodeID);
//
//                tree.pushLeft();
//                //traverse(callback, index, currentPath);
//                tree.pop();
//
//                tree.pushRight();
//                //traverse(callback, index, currentPath);
//                tree.pop();
//            }
//        }
//    }

    public int getNodeID()
    {
        return nodeID;
    }

    public int getLevel()
    {
        return level;
    }


    public void pushLeft()
    {
        nodeID *= 2;
        level++;
    }

    public void pushRight()
    {
        nodeID = nodeID * 2 + 1;
        level++;
    }

    public void pop()
    {
        nodeID /= 2;
        level--;
    }

    public boolean isLeafNode()
    {
        return nodeID >= leafNodeOffset;
    }

    public boolean nodeExists()
    {
        return nodeID - leafNodeOffset < leafNodeOffset;
    }
}
