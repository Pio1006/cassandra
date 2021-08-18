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

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.Descriptor;

import static org.junit.Assert.assertEquals;
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
    public void versionAAPerSSTableComponentIsParsedCorrectly() throws Throwable
    {
        Files.touch(new File(descriptor.baseFilename() + "-SAI_GroupComplete.db"));

        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor);

        assertEquals(Version.AA, indexDescriptor.version);
        assertTrue(indexDescriptor.hasComponent(IndexComponent.GROUP_COMPLETION_MARKER));
    }

    @Test
    public void versionAAPerIndexComponentIsParsedCorrectly() throws Throwable
    {
        Files.touch(new File(descriptor.baseFilename() + "-SAI_GroupComplete.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_test_index_ColumnComplete.db"));

        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor);
        indexDescriptor.registerIndex("test_index");

        assertEquals(Version.AA, indexDescriptor.version);
        assertTrue(indexDescriptor.hasComponent(IndexComponent.create(IndexComponent.Type.COLUMN_COMPLETION_MARKER, "test_index")));
    }

    @Test
    public void versionBAPerSSTableComponentIsParsedCorrectly() throws Throwable
    {
        Files.touch(new File(descriptor.baseFilename() + "-SAI+ba+GroupComplete.db"));

        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor);

        assertEquals(Version.BA, indexDescriptor.version);
        assertTrue(indexDescriptor.hasComponent(IndexComponent.GROUP_COMPLETION_MARKER));
    }

    @Test
    public void versionBAPerIndexComponentIsParsedCorrectly() throws Throwable
    {
        Files.touch(new File(descriptor.baseFilename() + "-SAI+ba+GroupComplete.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI+ba+test_index+ColumnComplete.db"));

        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor);
        indexDescriptor.registerIndex("test_index");

        assertEquals(Version.BA, indexDescriptor.version);
        assertTrue(indexDescriptor.hasComponent(IndexComponent.create(IndexComponent.Type.COLUMN_COMPLETION_MARKER, "test_index")));
    }

    @Test
    public void allVersionAAPerSSTableComponentsAreLoaded() throws Throwable
    {
        Files.touch(new File(descriptor.baseFilename() + "-SAI_GroupComplete.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_GroupMeta.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_TokenValues.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_OffsetsValues.db"));

        IndexDescriptor result = IndexDescriptor.create(descriptor);

        assertTrue(result.hasComponent(IndexComponent.GROUP_COMPLETION_MARKER));
        assertTrue(result.hasComponent(IndexComponent.GROUP_META));
        assertTrue(result.hasComponent(IndexComponent.TOKEN_VALUES));
        assertTrue(result.hasComponent(IndexComponent.OFFSETS_VALUES));
    }

    @Test
    public void allVersionAAPerIndexLiteralComponentsAreLoaded() throws Throwable
    {
        Files.touch(new File(descriptor.baseFilename() + "-SAI_GroupComplete.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_test_index_ColumnComplete.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_test_index_Meta.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_test_index_TermsData.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_test_index_PostingLists.db"));


        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor);
        indexDescriptor.registerIndex("test_index");

        assertTrue(indexDescriptor.hasComponent(IndexComponent.create(IndexComponent.Type.COLUMN_COMPLETION_MARKER, "test_index")));
        assertTrue(indexDescriptor.hasComponent(IndexComponent.create(IndexComponent.Type.META, "test_index")));
        assertTrue(indexDescriptor.hasComponent(IndexComponent.create(IndexComponent.Type.TERMS_DATA, "test_index")));
        assertTrue(indexDescriptor.hasComponent(IndexComponent.create(IndexComponent.Type.POSTING_LISTS, "test_index")));
    }

    @Test
    public void allVersionAAPerIndexNumericComponentsAreLoaded() throws Throwable
    {
        Files.touch(new File(descriptor.baseFilename() + "-SAI_GroupComplete.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_test_index_ColumnComplete.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_test_index_Meta.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_test_index_KDTree.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_test_index_KDTreePostingLists.db"));

        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor);
        indexDescriptor.registerIndex("test_index");

        assertTrue(indexDescriptor.hasComponent(IndexComponent.create(IndexComponent.Type.COLUMN_COMPLETION_MARKER, "test_index")));
        assertTrue(indexDescriptor.hasComponent(IndexComponent.create(IndexComponent.Type.META, "test_index")));
        assertTrue(indexDescriptor.hasComponent(IndexComponent.create(IndexComponent.Type.KD_TREE, "test_index")));
        assertTrue(indexDescriptor.hasComponent(IndexComponent.create(IndexComponent.Type.KD_TREE_POSTING_LISTS, "test_index")));
    }
}
