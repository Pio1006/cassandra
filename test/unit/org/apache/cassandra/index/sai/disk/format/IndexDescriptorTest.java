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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.Pair;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IndexDescriptorTest
{
    private TemporaryFolder temporaryFolder = new TemporaryFolder();
    private Descriptor descriptor;

    @BeforeClass
    public static void initialise()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup() throws Throwable
    {
        temporaryFolder.create();
        descriptor = Descriptor.fromFilename(temporaryFolder.newFolder().getAbsolutePath() + "/ca-1-bti-Data.db");
    }

    @After
    public void teardown()
    {
        temporaryFolder.delete();
    }

    @Test
    public void latestVersionDescriptorIsReturnedCorrectly() throws Throwable
    {
        IndexDescriptor latest = IndexDescriptor.latest(descriptor);

        assertEquals(latest.version, Version.LATEST);
        assertEquals(latest.descriptor, descriptor);
    }

    @Test
    public void versionAAPerSSTableComponentIsParsedCorrectly() throws Throwable
    {
        File file = new File(descriptor.baseFilename() + "-SAI_GroupMeta.db");

        Pair<Version, IndexComponent> pair = IndexDescriptor.fromFile(file);

        assertEquals(Version.AA, pair.left);
        assertEquals(IndexComponent.Type.GROUP_META, pair.right.type);
    }

    @Test
    public void versionAAPerIndexComponentIsParsedCorrectly() throws Throwable
    {
        File file = new File(descriptor.baseFilename() + "-SAI_test_index_Meta.db");

        Pair<Version, IndexComponent> pair = IndexDescriptor.fromFile(file);

        assertEquals(Version.AA, pair.left);
        assertEquals("test_index", pair.right.index);
        assertEquals(IndexComponent.Type.META, pair.right.type);
    }

    @Test
    public void versionAALegacyPerIndexComponentIsParsedCorrectly() throws Throwable
    {
        File file = new File(descriptor.baseFilename() + "-test_index_SAI_Meta.db");
        // Create the file so we can confirm that it has been renamed correctly
        Files.touch(file);

        assertTrue(file.exists());

        Pair<Version, IndexComponent> pair = IndexDescriptor.fromFile(file);

        assertEquals(Version.AA, pair.left);
        assertEquals("test_index", pair.right.index);
        assertEquals(IndexComponent.Type.META, pair.right.type);
        // The file should have been renamed to the correct AA per-index format
        assertFalse(file.exists());
        assertTrue(new File(descriptor.baseFilename() + "-SAI_test_index_Meta.db").exists());
    }

    @Test
    public void versionBAPerSSTableComponentIsParsedCorrectly() throws Throwable
    {
        File file = new File(descriptor.baseFilename() + "-SAI+ba+GroupMeta.db");

        Pair<Version, IndexComponent> pair = IndexDescriptor.fromFile(file);

        assertEquals(Version.BA, pair.left);
        assertEquals(IndexComponent.Type.GROUP_META, pair.right.type);
    }

    @Test
    public void versionBAPerIndexComponentIsParsedCorrectly() throws Throwable
    {
        File file = new File(descriptor.baseFilename() + "-SAI+ba+test_index+Meta.db");

        Pair<Version, IndexComponent> pair = IndexDescriptor.fromFile(file);

        assertEquals(Version.BA, pair.left);
        assertEquals("test_index", pair.right.index);
        assertEquals(IndexComponent.Type.META, pair.right.type);
    }

    @Test
    public void allVersionAAPerSSTableComponentsAreLoaded() throws Throwable
    {
        IndexDescriptor indexDescriptor = IndexDescriptor.forVersion(descriptor, Version.AA);
        Files.touch(indexDescriptor.fileFor(IndexComponent.GROUP_COMPLETION_MARKER));
        Files.touch(indexDescriptor.fileFor(IndexComponent.GROUP_META));
        Files.touch(indexDescriptor.fileFor(IndexComponent.TOKEN_VALUES));
        Files.touch(indexDescriptor.fileFor(IndexComponent.OFFSETS_VALUES));

        IndexDescriptor result = IndexDescriptor.forSSTable(descriptor);

        assertTrue(result.hasComponent(IndexComponent.GROUP_COMPLETION_MARKER));
        assertTrue(result.hasComponent(IndexComponent.GROUP_META));
        assertTrue(result.hasComponent(IndexComponent.TOKEN_VALUES));
        assertTrue(result.hasComponent(IndexComponent.OFFSETS_VALUES));
    }

    @Test
    public void allVersionAAPerIndexLiteralComponentsAreLoaded() throws Throwable
    {
        IndexDescriptor indexDescriptor = IndexDescriptor.forVersion(descriptor, Version.AA);
        IndexComponent columnCompletionMarker = IndexComponent.create(IndexComponent.Type.COLUMN_COMPLETION_MARKER, "test_index");
        IndexComponent meta = IndexComponent.create(IndexComponent.Type.META, "test_index");
        IndexComponent termsData = IndexComponent.create(IndexComponent.Type.TERMS_DATA, "test_index");
        IndexComponent postingLists = IndexComponent.create(IndexComponent.Type.POSTING_LISTS, "test_index");

        Files.touch(indexDescriptor.fileFor(columnCompletionMarker));
        Files.touch(indexDescriptor.fileFor(meta));
        Files.touch(indexDescriptor.fileFor(termsData));
        Files.touch(indexDescriptor.fileFor(postingLists));

        IndexDescriptor result = IndexDescriptor.forSSTable(descriptor);

        assertTrue(result.hasComponent(columnCompletionMarker));
        assertTrue(result.hasComponent(meta));
        assertTrue(result.hasComponent(termsData));
        assertTrue(result.hasComponent(postingLists));
    }

    @Test
    public void allVersionAAPerIndexNumericComponentsAreLoaded() throws Throwable
    {
        IndexDescriptor indexDescriptor = IndexDescriptor.forVersion(descriptor, Version.AA);
        IndexComponent columnCompletionMarker = IndexComponent.create(IndexComponent.Type.COLUMN_COMPLETION_MARKER, "test_index");
        IndexComponent meta = IndexComponent.create(IndexComponent.Type.META, "test_index");
        IndexComponent kdtree = IndexComponent.create(IndexComponent.Type.KD_TREE, "test_index");
        IndexComponent kdtreePostingLists = IndexComponent.create(IndexComponent.Type.KD_TREE_POSTING_LISTS, "test_index");

        Files.touch(indexDescriptor.fileFor(columnCompletionMarker));
        Files.touch(indexDescriptor.fileFor(meta));
        Files.touch(indexDescriptor.fileFor(kdtree));
        Files.touch(indexDescriptor.fileFor(kdtreePostingLists));

        IndexDescriptor result = IndexDescriptor.forSSTable(descriptor);

        assertTrue(result.hasComponent(columnCompletionMarker));
        assertTrue(result.hasComponent(meta));
        assertTrue(result.hasComponent(kdtree));
        assertTrue(result.hasComponent(kdtreePostingLists));
    }
}
