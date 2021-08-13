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
package org.apache.cassandra.index.sai.disk.v1.writers;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.disk.ColumnIndexWriter;
import org.apache.cassandra.index.sai.disk.ImmutableOneDimPointValues;
import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Column index writer that flushes indexed data directly from the corresponding Memtable index, without buffering index
 * data in memory.
 */
public class MemtableIndexWriter implements ColumnIndexWriter
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final MemtableIndex memtable;
    private final RowMapping rowMapping;
    private final ColumnContext context;
    private final IndexDescriptor indexDescriptor;
    private final IndexComponent meta;

    public MemtableIndexWriter(MemtableIndex memtable,
                               IndexDescriptor indexDescriptor,
                               ColumnContext context,
                               RowMapping rowMapping,
                               CompressionParams compressionParams)
    {
        assert rowMapping != null && rowMapping != RowMapping.DUMMY : "Row mapping must exist during FLUSH.";

        this.memtable = memtable;
        this.rowMapping = rowMapping;
        this.context = context;
        this.indexDescriptor = indexDescriptor;
        this.meta = IndexComponent.create(IndexComponent.Type.META, context.getIndexName());
    }

    @Override
    public void addRow(DecoratedKey rowKey, long ssTableRowId, Row row)
    {
        // Memtable indexes are flushed directly to disk with the aid of a mapping between primary
        // keys and row IDs in the flushing SSTable. This writer, therefore, does nothing in
        // response to the flushing of individual rows.
    }

    @Override
    public void abort(Throwable cause)
    {
        logger.warn(context.logMessage("Aborting index memtable flush for {}..."), indexDescriptor.descriptor, cause);
        indexDescriptor.deleteColumnIndex(context.getIndexName());
    }

    @Override
    public void flush() throws IOException
    {
        long start = System.nanoTime();

        try
        {
            if (!rowMapping.hasRows() || (memtable == null) || memtable.isEmpty())
            {
                logger.debug(context.logMessage("No indexed rows to flush from SSTable {}."), indexDescriptor.descriptor);
                // Write a completion marker even though we haven't written anything to the index
                // so we won't try to build the index again for the SSTable
                indexDescriptor.createComponentOnDisk(IndexComponent.create(IndexComponent.Type.COLUMN_COMPLETION_MARKER, context.getIndexName()));
                return;
            }

            final DecoratedKey minKey = rowMapping.minKey;
            final DecoratedKey maxKey = rowMapping.maxKey;

            final Iterator<Pair<ByteComparable, IntArrayList>> iterator = rowMapping.merge(memtable);

            try (MemtableTermsIterator terms = new MemtableTermsIterator(memtable.getMinTerm(), memtable.getMaxTerm(), iterator))
            {
                long cellCount = flush(minKey, maxKey, context.getValidator(), terms, rowMapping.maxSegmentRowId);

                indexDescriptor.createComponentOnDisk(IndexComponent.create(IndexComponent.Type.COLUMN_COMPLETION_MARKER, context.getIndexName()));

                context.getIndexMetrics().memtableIndexFlushCount.inc();

                long durationMillis = Math.max(1, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

                if (logger.isTraceEnabled())
                {
                    logger.trace(context.logMessage("Flushed {} Memtable index cells for {} in {} ms."), cellCount, indexDescriptor.descriptor, durationMillis);
                }

                context.getIndexMetrics().memtableFlushCellsPerSecond.update((long) (cellCount * 1000.0 / durationMillis));
            }
        }
        catch (Throwable t)
        {
            logger.error(context.logMessage("Error while flushing index {}"), t.getMessage(), t);
            context.getIndexMetrics().memtableIndexFlushErrors.inc();

            throw t;
        }
    }

    private long flush(DecoratedKey minKey, DecoratedKey maxKey, AbstractType<?> termComparator, MemtableTermsIterator terms, int maxSegmentRowId) throws IOException
    {
        long numRows;
        SegmentMetadata.ComponentMetadataMap indexMetas;

        if (TypeUtil.isLiteral(termComparator))
        {
            try (InvertedIndexWriter writer = new InvertedIndexWriter(indexDescriptor, context.getIndexName(), false))
            {
                indexMetas = writer.writeAll(terms);
                numRows = writer.getPostingsCount();
            }
        }
        else
        {
            try (NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
                                                                    context,
                                                                    TypeUtil.fixedSizeOf(termComparator),
                                                                    maxSegmentRowId,
                                                                    // Due to stale entries in IndexMemtable, we may have more indexed rows than num of rowIds.
                                                                    Integer.MAX_VALUE,
                                                                    context.getIndexWriterConfig(),
                                                                    false))
            {
                indexMetas = writer.writeAll(ImmutableOneDimPointValues.fromTermEnum(terms, termComparator));
                numRows = writer.getPointCount();
            }
        }

        // If no rows were written we need to delete any created column index components
        // so that the index is correctly identified as being empty (only having a completion marker)
        if (numRows == 0)
        {
            indexDescriptor.deleteColumnIndex(context.getIndexName());
            return 0;
        }

        // During index memtable flush, the data is sorted based on terms.
        SegmentMetadata metadata = new SegmentMetadata(0, numRows, terms.getMinSSTableRowId(), terms.getMaxSSTableRowId(),
                                                       minKey, maxKey, terms.getMinTerm(), terms.getMaxTerm(), indexMetas);

        try (MetadataWriter writer = new MetadataWriter(indexDescriptor.createOutput(meta, false, false)))
        {
            SegmentMetadata.write(writer, Collections.singletonList(metadata), null);
        }

        return numRows;
    }
}
