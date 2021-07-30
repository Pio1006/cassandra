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

package org.apache.cassandra.index.sai.utils;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.VersionedIndex;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;

/**
 * container to share per-index file handles(kdtree, terms data, posting lists) among segments.
 */
public class PerIndexFiles implements Closeable
{
    private final VersionedIndex versionedIndex;
    private final Map<IndexComponent.Type, FileHandle> files = new HashMap<>(2);

    public PerIndexFiles(VersionedIndex versionedIndex)
    {
        this(versionedIndex, false);
    }

    public PerIndexFiles(VersionedIndex versionedIndex, boolean temporary)
    {
        this.versionedIndex = versionedIndex;
        if (versionedIndex.isLiteral())
        {
            putFile(IndexComponent.Type.POSTING_LISTS, temporary);
            putFile(IndexComponent.Type.TERMS_DATA, temporary);
        }
        else
        {
            putFile(IndexComponent.Type.KD_TREE, temporary);
            putFile(IndexComponent.Type.KD_TREE_POSTING_LISTS, temporary);
        }
    }

    public FileHandle get(IndexComponent.Type type)
    {
        return getFile(type);
    }

    private void putFile(IndexComponent.Type type, boolean temporary)
    {
        files.put(type, IndexFileUtils.instance.createFileHandle(versionedIndex, type, temporary));
    }

    private FileHandle getFile(IndexComponent.Type type)
    {
        FileHandle file = files.get(type);
        if (file == null)
            throw new IllegalArgumentException(String.format("Component for %s not found for SSTable %s",
                                                             type.representation,
                                                             versionedIndex.descriptor()));

        return file;
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(files.values());
    }
}
