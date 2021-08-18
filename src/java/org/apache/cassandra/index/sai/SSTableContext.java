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

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.V1SSTableContext;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;
import org.apache.lucene.store.IndexInput;

/**
 * SSTableContext is created for individual sstable shared across indexes to track per-sstable index files.
 *
 * SSTableContext itself will be released when receiving sstable removed notification, but its shared copies in individual
 * SSTableIndex will be released when in-flight read requests complete.
 */
public abstract class SSTableContext extends SharedCloseableImpl
{
    public final SSTableReader sstable;
    public final IndexDescriptor indexDescriptor;

    protected SSTableContext(SSTableReader sstable, IndexDescriptor indexDescriptor, RefCounted.Tidy tidy)
    {
        super(tidy);
        this.sstable = sstable;
        this.indexDescriptor = indexDescriptor;
    }

    protected SSTableContext(SSTableReader sstable, IndexDescriptor indexDescriptor, SharedCloseableImpl copy)
    {
        super(copy);
        this.sstable = sstable;
        this.indexDescriptor = indexDescriptor;
    }

    @SuppressWarnings("resource")
    public static SSTableContext create(SSTableReader sstable, IndexDescriptor indexDescriptor)
    {
        // We currently only support the V1 per-sstable format but in future selection of
        // per-sstable format will be here
        return V1SSTableContext.create(sstable, indexDescriptor);
    }

    @Override
    public abstract SSTableContext sharedCopy();
    /**
     * @return number of open files per {@link SSTableContext} instance
     */
    public abstract int openFilesPerSSTable();

    /**
     * @return disk usage of per-sstable index files
     */
    public abstract long diskUsage();

    public abstract PerIndexFiles perIndexFiles(ColumnContext columnContext);

    public IndexInput openInput(IndexComponent indexComponent)
    {
        return indexDescriptor.openInput(indexComponent);
    }
}
