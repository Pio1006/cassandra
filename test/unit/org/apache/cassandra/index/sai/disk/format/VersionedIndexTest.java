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

import java.util.List;
import java.util.Map;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.Descriptor;

public class VersionedIndexTest
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
    public void multipleGroupCompletionMarkersOnSameSSTable() throws Throwable
    {
//        IndexDescriptor aaDescriptor = IndexDescriptor.forVersion(descriptor, Version.AA, 0);
//        Files.touch(aaDescriptor.fileFor(IndexComponent.GROUP_COMPLETION_MARKER));
//        IndexDescriptor baDescriptor = IndexDescriptor.forVersion(descriptor, Version.BA, 1);
//        Files.touch(baDescriptor.fileFor(IndexComponent.GROUP_COMPLETION_MARKER));
//
//        Map<IndexDescriptor, List<IndexComponent>> componentMap = IndexDescriptor.loadIndexComponents(descriptor);


        System.out.println();
    }

}
