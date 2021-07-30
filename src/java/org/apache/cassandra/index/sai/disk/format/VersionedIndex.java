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

package org.apache.cassandra.index.sai.disk.format;

import java.io.File;

import com.google.common.base.Strings;

import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;

public class VersionedIndex
{
    enum Type
    {
        NONE,
        LITERAL,
        NUMERIC
    }

    private final IndexDescriptor indexDescriptor;
    private final String index;
    private final Type type;

    public VersionedIndex(IndexDescriptor indexDescriptor)
    {
        this.indexDescriptor = indexDescriptor;
        this.index = null;
        this.type = Type.NONE;
    }

    public VersionedIndex(IndexDescriptor indexDescriptor, String index, boolean isLiteral)
    {
        this.indexDescriptor = indexDescriptor;
        this.index = index;
        this.type = isLiteral ? Type.LITERAL : Type.NUMERIC;
    }

    public VersionedIndex(IndexDescriptor indexDescriptor, String index, Type type)
    {
        this.indexDescriptor = indexDescriptor;
        this.index = index;
        this.type = type;
    }

    public static VersionedIndex create(Descriptor descriptor)
    {
        return new VersionedIndex(IndexDescriptor.forSSTable(descriptor), null, Type.NONE);
    }

    public static VersionedIndex create(Descriptor descriptor, String index)
    {
        //TODO We can't identify the type of index for this but we should be able to get a list of
        // index components
        return new VersionedIndex(IndexDescriptor.forIndex(descriptor, index), index, Type.NONE);
    }

    public VersionedIndex withIndex(String indexName)
    {
        return new VersionedIndex(indexDescriptor, indexName, type);
    }

    public IndexComponent component(IndexComponent.Type type)
    {
        return IndexComponent.create(type, index);
    }

    public IndexDescriptor indexDescriptor()
    {
        return indexDescriptor;
    }

    public boolean isLiteral()
    {
        return type == Type.LITERAL;
    }

    public Descriptor descriptor()
    {
        return indexDescriptor.descriptor();
    }

    public File tmpFileFor(IndexComponent.Type type)
    {
        return indexDescriptor.tmpFileFor(IndexComponent.create(type, index));
    }

    public File fileFor(IndexComponent.Type type)
    {
        return indexDescriptor.fileFor(IndexComponent.create(type, index));
    }

    public boolean isColumnIndexComplete()
    {
        return true;
    }

    public boolean isGroupIndexComplete()
    {
        //TODO Implement
        return true;
    }

    public void deletePerSSTableIndexComponents()
    {
        //TODO Implement
    }

    public void deleteColumnIndex()
    {
        //TODO Implement this
    }

    public void deleteTemporaryComponents()
    {
        //TODO Implement this
    }

    public void validatePerColumnComponents()
    {
    }

    public boolean validatePerColumnComponentsChecksum()
    {
        return true;
    }

    public void validatePerSSTableComponents()
    {

    }

    public boolean validatePerSSTableComponentsChecksum()
    {
        return true;
    }

    public String logMessage(String message)
    {
        // Index names are unique only within a keyspace.
        return String.format("[%s.%s.%s] %s",
                             indexDescriptor.descriptor().ksname,
                             indexDescriptor.descriptor().cfname,
                             Strings.isNullOrEmpty(index) ? "*" : index,
                             message);
    }
}
