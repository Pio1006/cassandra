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
