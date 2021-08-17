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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.sai.disk.PerSSTableComponentsWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.lucene.util.IOUtils;

public class SSTableComponentsWriter implements PerSSTableComponentsWriter
{
    protected static final Logger logger = LoggerFactory.getLogger(PerSSTableComponentsWriter.class);

    private final NumericValuesWriter tokenWriter;
    private final NumericValuesWriter offsetWriter;
    private final MetadataWriter metadataWriter;

    private final IndexDescriptor indexDescriptor;

    private DecoratedKey currentKey;

    private long currentKeyPartitionOffset;

    public SSTableComponentsWriter(IndexDescriptor indexDescriptor, CompressionParams compressionParams) throws IOException
    {
        this.indexDescriptor = indexDescriptor;

        this.metadataWriter = new MetadataWriter(indexDescriptor.openOutput(IndexComponent.GROUP_META, false, false));

        this.tokenWriter = new NumericValuesWriter(IndexComponent.TOKEN_VALUES,
                                                   indexDescriptor.openOutput(IndexComponent.TOKEN_VALUES, false, false),
                                                   metadataWriter, false);
        this.offsetWriter = new NumericValuesWriter(IndexComponent.OFFSETS_VALUES,
                                                    indexDescriptor.openOutput(IndexComponent.OFFSETS_VALUES, false, false),
                                                    metadataWriter, true);
    }

//    private SSTableComponentsWriter()
//    {
//        this.indexDescriptor = null;
//        this.metadataWriter = null;
//        this.tokenWriter = null;
//        this.offsetWriter = null;
//    }

    public void startPartition(DecoratedKey key, long position)
    {
        currentKey = key;
        currentKeyPartitionOffset = position;
    }

    public void nextUnfilteredCluster(Unfiltered unfiltered, long position) throws IOException
    {
        recordCurrentTokenOffset();
    }

    public void staticRow(Row staticRow, long position) throws IOException
    {
        recordCurrentTokenOffset();
    }

    private void recordCurrentTokenOffset() throws IOException
    {
        recordCurrentTokenOffset((long) currentKey.getToken().getTokenValue(), currentKeyPartitionOffset);
    }

    @VisibleForTesting
    public void recordCurrentTokenOffset(long tokenValue, long keyOffset) throws IOException
    {
        tokenWriter.add(tokenValue);
        offsetWriter.add(keyOffset);
    }

    public void complete() throws IOException
    {
        IOUtils.close(tokenWriter, offsetWriter, metadataWriter);
        indexDescriptor.createComponentOnDisk(IndexComponent.GROUP_COMPLETION_MARKER);
    }

    public void abort(Throwable accumulator)
    {
        logger.debug(indexDescriptor.logMessage("Aborting token/offset writer for {}..."), indexDescriptor.descriptor);
        indexDescriptor.deletePerSSTableIndexComponents();
    }
}
