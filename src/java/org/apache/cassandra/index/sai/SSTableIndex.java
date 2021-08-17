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

package org.apache.cassandra.index.sai;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.disk.v1.Segment;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.V1SSTableContext;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.utils.RangeConcatIterator;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;


/**
 * SSTableIndex is created for each column index on individual sstable to track per-column indexer.
 */
public class SSTableIndex
{
    // sort sstable index by first key then last key
    public static final Comparator<SSTableIndex> COMPARATOR = Comparator.comparing((SSTableIndex s) -> s.getSSTable().first)
                                                                        .thenComparing(s -> s.getSSTable().last)
                                                                        .thenComparing(s -> s.getSSTable().descriptor.generation);

    private final SSTableContext sstableContext;
    private final ColumnContext columnContext;
    private final SSTableReader sstable;

    private final ImmutableList<Segment> segments;
    private PerIndexFiles indexFiles;

    private final List<SegmentMetadata> metadatas;
    private final DecoratedKey minKey, maxKey; // in token order
    private final ByteBuffer minTerm, maxTerm;
    private final long minSSTableRowId, maxSSTableRowId;
    private final long numRows;

    private final AtomicInteger references = new AtomicInteger(1);
    private final AtomicBoolean obsolete = new AtomicBoolean(false);

    public SSTableIndex(SSTableContext sstableContext, ColumnContext columnContext)
    {
        this.sstableContext = sstableContext.sharedCopy();
        this.columnContext = columnContext;
        this.sstable = sstableContext.sstable;

        final AbstractType<?> validator = columnContext.getValidator();
        assert validator != null;

        try
        {
            this.indexFiles = sstableContext.perIndexFiles(columnContext);

            ImmutableList.Builder<Segment> segmentsBuilder = ImmutableList.builder();

            final MetadataSource source = MetadataSource.load(sstableContext.openInput(IndexComponent.create(IndexComponent.Type.META, columnContext.getIndexName())));
            metadatas = SegmentMetadata.load(source, null);

            for (SegmentMetadata metadata : metadatas)
            {
                segmentsBuilder.add(new Segment(columnContext, (V1SSTableContext)sstableContext, indexFiles, metadata));
            }

            segments = segmentsBuilder.build();
            assert !segments.isEmpty();

            this.minKey = metadatas.get(0).minKey;
            this.maxKey = metadatas.get(metadatas.size() - 1).maxKey;

            this.minTerm = metadatas.stream().map(m -> m.minTerm).min(TypeUtil.comparator(validator)).orElse(null);
            this.maxTerm = metadatas.stream().map(m -> m.maxTerm).max(TypeUtil.comparator(validator)).orElse(null);

            this.numRows = metadatas.stream().mapToLong(m -> m.numRows).sum();

            this.minSSTableRowId = metadatas.get(0).minSSTableRowId;
            this.maxSSTableRowId = metadatas.get(metadatas.size() - 1).maxSSTableRowId;
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(indexFiles);
            FileUtils.closeQuietly(sstableContext);
            throw Throwables.unchecked(t);
        }
    }

    public ColumnContext getColumnContext()
    {
        return columnContext;
    }

    public SSTableContext getSSTableContext()
    {
        return sstableContext;
    }

    public long indexFileCacheSize()
    {
        return segments.stream().mapToLong(Segment::indexFileCacheSize).sum();
    }

    /**
     * @return number of indexed rows, note that rows may have been updated or removed in sstable.
     */
    public long getRowCount()
    {
        return numRows;
    }

    /**
     * @return total size of per-column index components, in bytes
     */
    public long sizeOfPerColumnComponents()
    {
        return sstableContext.indexDescriptor.sizeOfPerColumnComponents(columnContext.getIndexName());
    }

    /**
     * @return the smallest possible sstable row id in this index.
     */
    public long minSSTableRowId()
    {
        return minSSTableRowId;
    }

    /**
     * @return the largest possible sstable row id in this index.
     */
    public long maxSSTableRowId()
    {
        return maxSSTableRowId;
    }

    public ByteBuffer minTerm()
    {
        return minTerm;
    }

    public ByteBuffer maxTerm()
    {
        return maxTerm;
    }

    public DecoratedKey minKey()
    {
        return minKey;
    }

    public DecoratedKey maxKey()
    {
        return maxKey;
    }

    public RangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange, SSTableQueryContext context, boolean defer)
    {
        RangeConcatIterator.Builder builder = RangeConcatIterator.builder();

        for (Segment segment : segments)
        {
            if (segment.intersects(keyRange))
            {
                builder.add(segment.search(expression, context, defer));
            }
        }

        return builder.build();
    }

    public int getSegmentSize()
    {
        return segments.size();
    }

    public List<SegmentMetadata> segments()
    {
        return metadatas;
    }

    public Version getVersion()
    {
        return sstableContext.indexDescriptor.version;
    }

    public SSTableReader getSSTable()
    {
        return sstable;
    }

    public boolean reference()
    {
        while (true)
        {
            int n = references.get();
            if (n <= 0)
                return false;
            if (references.compareAndSet(n, n + 1))
            {
                return true;
            }
        }
    }

    public boolean isReleased()
    {
        return references.get() <= 0;
    }

    public void release()
    {
        int n = references.decrementAndGet();

        if (n == 0)
        {
            FileUtils.closeQuietly(indexFiles);
            FileUtils.closeQuietly(segments);
            sstableContext.close();

            /*
             * When SSTable is removed, storage-attached index components will be automatically removed by LogTransaction.
             * We only remove index components explicitly in case of index corruption or index rebuild.
             */
            if (obsolete.get())
            {
                sstableContext.deleteColumnIndex(columnContext.getIndexName());
            }
        }
    }

    public void markObsolete()
    {
        obsolete.getAndSet(true);
        release();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SSTableIndex other = (SSTableIndex)o;
        return Objects.equal(sstableContext, other.sstableContext) && Objects.equal(columnContext, other.columnContext);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(sstableContext, columnContext);
    }

    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("column", columnContext.getColumnName())
                          .add("index", columnContext.getIndexName())
                          .add("sstable", sstable.descriptor)
                          .add("totalRows", sstable.getTotalRows())
                          .toString();
    }
}
