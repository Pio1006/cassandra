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

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

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
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.lucene.store.IndexInput;

import static org.apache.cassandra.index.sai.StorageAttachedIndex.SEGMENT_BUILD_MEMORY_LIMITER;
import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;

public class V1OnDiskFormat implements OnDiskFormat
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final EnumSet<IndexComponent.Type> LITERAL_COMPONENT_TYPES = EnumSet.of(IndexComponent.Type.COLUMN_COMPLETION_MARKER,
                                                                                           IndexComponent.Type.META,
                                                                                           IndexComponent.Type.TERMS_DATA,
                                                                                           IndexComponent.Type.POSTING_LISTS);
    private static final EnumSet<IndexComponent.Type> NUMERIC_COMPONENT_TYPES = EnumSet.of(IndexComponent.Type.COLUMN_COMPLETION_MARKER,
                                                                                           IndexComponent.Type.META,
                                                                                           IndexComponent.Type.KD_TREE,
                                                                                           IndexComponent.Type.KD_TREE_POSTING_LISTS);

    public static final V1OnDiskFormat instance = new V1OnDiskFormat();

    private V1OnDiskFormat()
    {}

    @Override
    public boolean isGroupIndexComplete(IndexDescriptor indexDescriptor)
    {
        return indexDescriptor.registerSSTable().hasComponent(IndexComponent.GROUP_COMPLETION_MARKER);
    }

    @Override
    public boolean isColumnIndexComplete(IndexDescriptor indexDescriptor, String index)
    {
        indexDescriptor.registerSSTable().registerIndex(index);
        return indexDescriptor.hasComponent(IndexComponent.GROUP_COMPLETION_MARKER) &&
               indexDescriptor.hasComponent(IndexComponent.create(IndexComponent.Type.COLUMN_COMPLETION_MARKER, index));
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

    @Override
    public void validateComponent(IndexDescriptor indexDescriptor, IndexComponent indexComponent, boolean checksum) throws IOException
    {
        File file = indexDescriptor.fileFor(indexComponent);
        if (file.exists() && file.length() > 0)
        {
            try (IndexInput input = indexDescriptor.openInput(indexComponent))
            {
                if (checksum)
                    SAICodecUtils.validateChecksum(input);
                else
                    SAICodecUtils.validate(input);
            }
            catch (IOException e)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug(indexDescriptor.logMessage("{} failed for index component {} on SSTable {}"),
                                 (checksum ? "Checksum validation" : "Validation"),
                                 indexComponent,
                                 indexDescriptor.descriptor);
                }
                throw e;
            }
        }
    }

    @Override
    public Set<IndexComponent> perSSTableComponents()
    {
        return IndexComponent.PER_SSTABLE;
    }

    @Override
    public Set<IndexComponent> perIndexComponents(String index, boolean isLiteral)
    {
        return isLiteral ? LITERAL_COMPONENT_TYPES.stream().map(t -> IndexComponent.create(t, index)).collect(Collectors.toSet())
                         : NUMERIC_COMPONENT_TYPES.stream().map(t -> IndexComponent.create(t, index)).collect(Collectors.toSet());
    }
}
