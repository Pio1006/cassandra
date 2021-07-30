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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Strings;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.Pair;

public class IndexDescriptor
{
    public static final String SAI_DESCRIPTOR = "SAI";

    private static final String SEPARATOR = "-";
    private static final String EXTENSION = ".db";
    private static final String TMP_EXTENSION = ".tmp";

    // Version AA - The final 6.8 file format
    private static final String VERSION_AA_REGEX_STR = ".*SAI_((.*)_)?(.*).db";
    private static final Pattern VERSION_AA_REGEX = Pattern.compile(VERSION_AA_REGEX_STR);
    private static final String VERSION_AA_PER_SSTABLE_FORMAT = "SAI_%s.db";
    private static final String VERSION_AA_PER_INDEX_FORMAT = "SAI_%s_%s.db";

    // Version AA Legacy - This is the orginal 6.8 format that was per-column
    private static final String VERSION_AA_LEGACY_REGEX_STR = ".*-((.*)_)?SAI_(.*).db";
    private static final Pattern VERSION_AA_LEGACY_REGEX = Pattern.compile(VERSION_AA_LEGACY_REGEX_STR);

    // Version BA - Stargazer format
    private static final String VERSION_BA_REGEX_STR = ".*-SAI-([a-z][a-z])-(\\d*)-(.*).db";
    private static final Pattern VERSION_BA_REGEX = Pattern.compile(VERSION_BA_REGEX_STR);

    private final Version version;
    private final int generation;
    private final Descriptor descriptor;

    private IndexDescriptor(Version version, int generation, Descriptor descriptor)
    {
        this.version = version;
        this.generation = generation;
        this.descriptor = descriptor;
    }

    public static IndexDescriptor forSSTable(Descriptor descriptor)
    {
        for (File file : descriptor.directory.listFiles())
        {
            String filename = file.getName();
            if (filename.contains(SAI_DESCRIPTOR))
            {
                Matcher matcher = VERSION_AA_REGEX.matcher(filename);
                if (matcher.matches())
                {
                    return new IndexDescriptor(Version.AA, 0, descriptor);
                }
                matcher = VERSION_BA_REGEX.matcher(filename);
                if (matcher.matches())
                {
                    return new IndexDescriptor(Version.parse(matcher.group(1)), Integer.parseInt(matcher.group(2)), descriptor);
                }
            }
        }
        return new IndexDescriptor(Version.LATEST, 1, descriptor);
    }

    public static IndexDescriptor forIndex(Descriptor descriptor, String indexName)
    {
        //TODO Needs to identify indexName
        return forSSTable(descriptor);
    }

    public static IndexDescriptor latest(Descriptor descriptor)
    {
        return new IndexDescriptor(Version.LATEST, 1, descriptor);
    }

    public static IndexDescriptor forComponent(Descriptor descriptor, IndexComponent component)
    {
        //TODO This needs to identify the component and return the correct descriptor
        for (File file : descriptor.directory.listFiles())
        {
            String filename = file.getName();
            if (filename.contains(SAI_DESCRIPTOR))
            {
                Matcher matcher = VERSION_AA_REGEX.matcher(filename);
                if (matcher.matches())
                {
                    return new IndexDescriptor(Version.AA, 0, descriptor);
                }
                matcher = VERSION_BA_REGEX.matcher(filename);
                if (matcher.matches())
                {
                    return new IndexDescriptor(Version.parse(matcher.group(1)), Integer.parseInt(matcher.group(2)), descriptor);
                }
            }
        }
        return new IndexDescriptor(Version.LATEST, 1, descriptor);
    }

    public static Pair<IndexDescriptor, Component> fromFilenameWithComponent(File file)
    {
        String filename = file.getName();
        if (filename.contains(SAI_DESCRIPTOR))
        {
            Matcher matcher = VERSION_AA_REGEX.matcher(filename);
            if (matcher.matches())
            {
                String index = matcher.group(2);
                String type = matcher.group(3);

                return Pair.create(null, null);
            }
        }
        return null;
    }

    public Version version()
    {
        return version;
    }

    public Descriptor descriptor()
    {
        return descriptor;
    }

    public File tmpFileFor(IndexComponent component)
    {
        return new File(tmpFilenameFor(component));
    }

    public File fileFor(IndexComponent component)
    {
        return new File(filenameFor(component));
    }

    public String tmpFilenameFor(IndexComponent component)
    {
        return filenameFor(component) + TMP_EXTENSION;
    }

    public String filenameFor(IndexComponent component)
    {
//        assert component.type.perSSTable;

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(descriptor.baseFilename());

        if (version.onOrAfter(Version.BA))
        {
            stringBuilder.append(SEPARATOR).append(SAI_DESCRIPTOR);
            stringBuilder.append(SEPARATOR).append(version);
            stringBuilder.append(SEPARATOR).append(generation);
            stringBuilder.append(SEPARATOR).append(component.name);
            stringBuilder.append(EXTENSION);
        }
        return stringBuilder.toString();
    }

    public String logMessage(IndexComponent component, String message)
    {
        // Index names are unique only within a keyspace.
        return String.format("[%s.%s.%s] %s",
                             descriptor.ksname,
                             descriptor.cfname,
                             Strings.isNullOrEmpty(component.index) ? "*" : component.index,
                             message);
    }

    public String logMessage(String message)
    {
        // Index names are unique only within a keyspace.
        return String.format("[%s.%s.*] %s",
                             descriptor.ksname,
                             descriptor.cfname,
                             message);
    }
}
