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
package org.apache.cassandra.index.sai.disk;

import java.io.IOException;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;

/**
 * Writes all SSTable-attached index token and offset structures.
 */
public interface PerSSTableComponentsWriter
{
    public static final PerSSTableComponentsWriter NONE = new PerSSTableComponentsWriter()
    {
        @Override
        public void nextUnfilteredCluster(Unfiltered unfiltered, long position)
        {
        }

        @Override
        public void startPartition(DecoratedKey key, long position)
        {
        }

        @Override
        public void staticRow(Row staticRow, long position)
        {
        }

        @Override
        public void complete()
        {
        }

        @Override
        public void abort(Throwable accumulate)
        {
        }
    };

    public void nextUnfilteredCluster(Unfiltered unfiltered, long position) throws IOException;

    public void startPartition(DecoratedKey key, long position);

    public void staticRow(Row staticRow, long position) throws IOException;

    public void complete() throws IOException;

    public void abort(Throwable accumulate);
}
