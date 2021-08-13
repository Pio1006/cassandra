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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.ColumnIndexWriter;
import org.apache.cassandra.index.sai.disk.PerSSTableComponentsWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v1.writers.MemtableIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.writers.SSTableComponentsWriter;
import org.apache.cassandra.index.sai.disk.v1.writers.SSTableIndexWriter;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.schema.CompressionParams;

import static org.apache.cassandra.index.sai.StorageAttachedIndex.SEGMENT_BUILD_MEMORY_LIMITER;
import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;

public class V1OnDiskFormat implements OnDiskFormat
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final V1OnDiskFormat instance = new V1OnDiskFormat();

    private V1OnDiskFormat()
    {}

    @Override
    public boolean isGroupIndexComplete(IndexDescriptor indexDescriptor)
    {
        return indexDescriptor.hasComponent(IndexComponent.GROUP_COMPLETION_MARKER);
    }

    public PerSSTableComponentsWriter createPerSSTableComponentsWriter(boolean perColumnOnly,
                                                                       IndexDescriptor indexDescriptor,
                                                                       CompressionParams compressionParams) throws IOException
    {
        return perColumnOnly ? PerSSTableComponentsWriter.NONE : new SSTableComponentsWriter(indexDescriptor, compressionParams);
    }

    @Override
    public ColumnIndexWriter newIndexWriter(StorageAttachedIndex index,
                                            IndexDescriptor indexDescriptor,
                                            LifecycleNewTracker tracker,
                                            RowMapping rowMapping,
                                            CompressionParams compressionParams)
    {
        // If we're not flushing or we haven't yet started the initialization build, flush from SSTable contents.
        if (tracker.opType() != OperationType.FLUSH || !index.isInitBuildStarted())
        {
            NamedMemoryLimiter limiter = SEGMENT_BUILD_MEMORY_LIMITER;
            logger.info(index.getContext().logMessage("Starting a compaction index build. Global segment memory usage: {}"), prettyPrintMemory(limiter.currentBytesUsed()));

            return new SSTableIndexWriter(indexDescriptor, index.getContext(), limiter, index.isIndexValid(), compressionParams);
        }

        return new MemtableIndexWriter(index.getContext().getPendingMemtableIndex(tracker), indexDescriptor, index.getContext(), rowMapping, compressionParams);
    }
}
