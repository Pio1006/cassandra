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
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.ColumnIndexWriter;
import org.apache.cassandra.index.sai.disk.PerSSTableComponentsWriter;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.lucene.store.IndexInput;

public class IndexDescriptor
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String SAI_DESCRIPTOR = "SAI";

    private static final String SEPARATOR = "-";
    private static final String SAI_SEPARATOR = "+";
    private static final String EXTENSION = ".db";
    private static final String TMP_EXTENSION = ".tmp";

    // Version AA - The final 6.8 file format
    private static final String VERSION_AA_REGEX_STR = ".*-SAI_((.*)_)?(.*).db";
    private static final Pattern VERSION_AA_REGEX = Pattern.compile(VERSION_AA_REGEX_STR);
    private static final String VERSION_AA_PER_SSTABLE_FORMAT = "SAI_%s.db";
    private static final String VERSION_AA_PER_INDEX_FORMAT = "SAI_%s_%s.db";

    // Version AA Legacy - This is the orginal 6.8 format for per-column components
    private static final String VERSION_AA_LEGACY_REGEX_STR = ".*-((.*)_)?SAI_(.*).db";
    private static final Pattern VERSION_AA_LEGACY_REGEX = Pattern.compile(VERSION_AA_LEGACY_REGEX_STR);

    // Version BA - Stargazer format
    private static final String VERSION_BA_REGEX_STR = ".*-SAI\\+([a-z][a-z])\\+((.*)\\+)?(.*).db";
    private static final Pattern VERSION_BA_REGEX = Pattern.compile(VERSION_BA_REGEX_STR);

    public final Version version;
    public final Descriptor descriptor;
    public final Set<IndexComponent> perSSTableComponents;
    public final Map<String, Set<IndexComponent>> perIndexComponents;

    private IndexDescriptor(Version version, Descriptor descriptor)
    {
        this.version = version;
        this.descriptor = descriptor;
        this.perSSTableComponents = Sets.newHashSet();
        this.perIndexComponents = Maps.newHashMap();
    }

    private IndexDescriptor(Version version,
                            Descriptor descriptor,
                            Set<IndexComponent> perSSTableComponents,
                            Map<String, Set<IndexComponent>> perIndexComponents)
    {
        this.version = version;
        this.descriptor = descriptor;
        this.perSSTableComponents = perSSTableComponents;
        this.perIndexComponents = perIndexComponents;
    }

    public static IndexDescriptor forSSTable(Descriptor descriptor)
    {
        Set<IndexComponent> perSSTableComponents = Sets.newHashSet();
        Map<String, Set<IndexComponent>> perIndexComponents = Maps.newHashMap();
        Version version = null;
        for (File file : descriptor.directory.listFiles())
        {
            if (file.getName().contains(SAI_DESCRIPTOR))
            {
                Pair<Version, IndexComponent> versionedComponent = fromFile(file);
                if (versionedComponent != null)
                {
                    if (version == null)
                        version = versionedComponent.left;
                    // All components must be of the same version. If we have a mismatch then return
                    // the latest version with no components
                    else if (!version.equals(versionedComponent.left))
                        return latest(descriptor);

                    if (versionedComponent.right.type.perSSTable)
                        perSSTableComponents.add(versionedComponent.right);
                    else
                        perIndexComponents.computeIfAbsent(versionedComponent.right.index, k -> Sets.newHashSet()).add(versionedComponent.right);
                }
            }
        }
        return new IndexDescriptor(version == null ? Version.LATEST: version, descriptor, perSSTableComponents, perIndexComponents);
    }

    @VisibleForTesting
    public static IndexDescriptor forVersion(Descriptor descriptor, Version version)
    {
        return new IndexDescriptor(version, descriptor);
    }

    public static IndexDescriptor latest(Descriptor descriptor)
    {
        Preconditions.checkArgument(descriptor != null, "Descriptor can't be null");

        return new IndexDescriptor(Version.LATEST, descriptor);
    }

    public static Pair<IndexDescriptor, IndexComponent> fromFilenameWithComponent(File file)
    {
        Preconditions.checkArgument(file != null, "File cannot be null");

        String filename = file.getName();
        if (filename.contains(SAI_DESCRIPTOR))
        {
            Descriptor descriptor = Descriptor.fromFilename(file);

            Matcher matcher = VERSION_BA_REGEX.matcher(filename);
            if (matcher.matches())
            {
                Version version = Version.parse(matcher.group(1));
                String index = matcher.group(3);
                IndexComponent.Type type = IndexComponent.Type.fromRepresentation(matcher.group(4));

                return Pair.create(new IndexDescriptor(version, descriptor),
                                   index == null ? IndexComponent.create(type) : IndexComponent.create(type, index));

            }
            matcher = VERSION_AA_REGEX.matcher(filename);
            if (matcher.matches())
            {
                String index = matcher.group(2);
                IndexComponent.Type type = IndexComponent.Type.fromRepresentation(matcher.group(3));

                return Pair.create(new IndexDescriptor(Version.AA, descriptor),
                                   StringUtils.isEmpty(index) ? IndexComponent.create(type) : IndexComponent.create(type, index));
            }
            matcher = VERSION_AA_LEGACY_REGEX.matcher(filename);
            if (matcher.matches())
            {
                String index = matcher.group(2);
                IndexComponent.Type type = IndexComponent.Type.fromRepresentation(matcher.group(3));
                IndexComponent indexComponent = StringUtils.isEmpty(index) ? IndexComponent.create(type) : IndexComponent.create(type, index);
                IndexDescriptor indexDescriptor = new IndexDescriptor(Version.AA, descriptor);
                // If we identify any legacy named per-index components rename them immediately
                if (file.exists() && !indexComponent.type.perSSTable)
                    FileUtils.renameWithConfirm(file, indexDescriptor.fileFor(IndexComponent.create(type, index)));
                return Pair.create(new IndexDescriptor(Version.AA, descriptor),
                                   StringUtils.isEmpty(index) ? IndexComponent.create(type) : IndexComponent.create(type, index));
            }
        }
        return null;
    }

    public static Pair<Version, IndexComponent> fromFile(File file)
    {
        Preconditions.checkArgument(file != null, "File cannot be null");

        String filename = file.getName();
        if (filename.contains(SAI_DESCRIPTOR))
        {
            Descriptor descriptor = Descriptor.fromFilename(file);

            Matcher matcher = VERSION_BA_REGEX.matcher(filename);
            if (matcher.matches())
            {
                Version version = Version.parse(matcher.group(1));
                String index = matcher.group(3);
                IndexComponent.Type type = IndexComponent.Type.fromRepresentation(matcher.group(4));
                return Pair.create(version, StringUtils.isEmpty(index) ? IndexComponent.create(type) : IndexComponent.create(type, index));

            }
            matcher = VERSION_AA_REGEX.matcher(filename);
            if (matcher.matches())
            {
                String index = matcher.group(2);
                IndexComponent.Type type = IndexComponent.Type.fromRepresentation(matcher.group(3));

                return Pair.create(Version.AA, StringUtils.isEmpty(index) ? IndexComponent.create(type) : IndexComponent.create(type, index));
            }
            matcher = VERSION_AA_LEGACY_REGEX.matcher(filename);
            if (matcher.matches())
            {
                String index = matcher.group(2);
                IndexComponent.Type type = IndexComponent.Type.fromRepresentation(matcher.group(3));
                IndexComponent indexComponent = StringUtils.isEmpty(index) ? IndexComponent.create(type) : IndexComponent.create(type, index);
                IndexDescriptor indexDescriptor = new IndexDescriptor(Version.AA, descriptor);
                // If we identify any legacy named per-index components rename them immediately
                if (file.exists() && !indexComponent.type.perSSTable)
                    FileUtils.renameWithConfirm(file, indexDescriptor.fileFor(IndexComponent.create(type, index)));
                return Pair.create(Version.AA, StringUtils.isEmpty(index) ? IndexComponent.create(type) : IndexComponent.create(type, index));
            }
        }
        //TODO Probably want to log this or raise exception - see what SSTable does
        return null;
    }

    public boolean hasComponent(IndexComponent indexComponent)
    {
        if (indexComponent.type.perSSTable)
            return perSSTableComponents.contains(indexComponent);
        else if (perIndexComponents.containsKey(indexComponent.index))
            return perIndexComponents.get(indexComponent.index).contains(indexComponent);
        return false;
    }

    public int numberOfComponents(String indexName)
    {
        return perIndexComponents.containsKey(indexName) ? perIndexComponents.get(indexName).size() : 0;
    }

    public File tmpFileFor(IndexComponent component)
    {
        return new File(tmpFilenameFor(component));
    }

    public File fileFor(IndexComponent component)
    {
        return new File(filenameFor(component));
    }

    private String tmpFilenameFor(IndexComponent component)
    {
        return filenameFor(component) + TMP_EXTENSION;
    }

    private String filenameFor(IndexComponent component)
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(descriptor.baseFilename());

        if (version.onOrAfter(Version.BA))
        {
            stringBuilder.append(SEPARATOR).append(SAI_DESCRIPTOR);
            stringBuilder.append(SAI_SEPARATOR).append(version);
            if (!component.type.perSSTable)
                stringBuilder.append(SAI_SEPARATOR).append(component.index);
            stringBuilder.append(SAI_SEPARATOR).append(component.type.representation);
            stringBuilder.append(EXTENSION);
        }
        else if (version.equals(Version.AA))
        {
            stringBuilder.append(SEPARATOR)
                         .append(component.type.perSSTable ? String.format(VERSION_AA_PER_SSTABLE_FORMAT, component.type.representation)
                                                           : String.format(VERSION_AA_PER_INDEX_FORMAT, component.index, component.type.representation));
        }
        return stringBuilder.toString();
    }

    public PerSSTableComponentsWriter newPerSSTableComponentsWriter(boolean perColumnOnly,
                                                                    CompressionParams compressionParams) throws IOException
    {
        return version.onDiskFormat().createPerSSTableComponentsWriter(perColumnOnly, this, compressionParams);
    }

    public ColumnIndexWriter newIndexWriter(StorageAttachedIndex index,
                                            LifecycleNewTracker tracker,
                                            RowMapping rowMapping,
                                            CompressionParams compressionParams)
    {
        return version.onDiskFormat().newIndexWriter(index, this, tracker, rowMapping, compressionParams);
    }

    public boolean isGroupIndexComplete()
    {
        return version.onDiskFormat().isGroupIndexComplete(this);
    }

    public boolean isColumnIndexComplete(String indexName)
    {
        return hasComponent(IndexComponent.create(IndexComponent.Type.COLUMN_COMPLETION_MARKER, indexName));
    }

    public boolean isColumnIndexEmpty(String indexName)
    {
        return isColumnIndexComplete(indexName) && numberOfComponents(indexName) == 1;
    }

    public void validatePerSSTableComponents()
    {
        //TODO Delegate to OnDiskFormat
    }

    public boolean validatePerSSTableComponentsChecksum()
    {
        //TODO Delegate to OnDiskFormat
        return true;
    }

    public void deletePerSSTableIndexComponents()
    {
        //TODO Delegate to OnDiskFormat (or here)
    }

    public void deleteColumnIndex(String index)
    {
        //TODO Delegate to OnDiskFormat (or we can probably do this here)
    }

    public void deleteTemporaryComponents()
    {

    }

    public void createComponentOnDisk(IndexComponent component) throws IOException
    {
        Files.touch(fileFor(component));
    }

    public IndexInput openInput(IndexComponent indexComponent)
    {
        final File file = fileFor(indexComponent);
        if (logger.isTraceEnabled())
            logger.trace(logMessage("Opening blocking index input for file {} ({})"),
                         file,
                         FBUtilities.prettyPrintMemory(file.length()));

        return IndexFileUtils.instance.openBlockingInput(file);
    }

    public IndexOutputWriter createOutput(IndexComponent component, boolean append, boolean temporary) throws IOException
    {
        final File file = temporary ? tmpFileFor(component) : fileFor(component);

        if (logger.isTraceEnabled())
            logger.trace(logMessage(component, "Creating {} sstable attached index output for component {} on file {}..."),
                         temporary ? "temporary" : "",
                         component.type,
                         file);

        IndexOutputWriter writer = IndexFileUtils.instance.createOutput(file);

        if (append)
        {
            writer.skipBytes(file.length());
        }

        return writer;
    }

    public FileHandle createFileHandle(IndexComponent component, boolean temporary)
    {
        final File file = temporary ? tmpFileFor(component) : fileFor(component);

        if (logger.isTraceEnabled())
        {
            logger.trace(logMessage(component, "Opening {} file handle for {} ({})"),
                         temporary ? "temporary" : "", file, FBUtilities.prettyPrintMemory(file.length()));
        }

        try (final FileHandle.Builder builder = new FileHandle.Builder(file.getAbsolutePath()).mmapped(true))
        {
            return builder.complete();
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(descriptor, version);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexDescriptor other = (IndexDescriptor)o;
        return Objects.equal(descriptor, other.descriptor) &&
               Objects.equal(version, other.version);
    }

    @Override
    public String toString()
    {
        if (version.onOrAfter(Version.BA))
            return descriptor.toString() + "-SAI+" + version;
        else
            return descriptor.toString() + "-SAI";
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
