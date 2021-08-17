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

import java.io.File;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.trieindex.TrieIndexFormat;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.index.sai.disk.FileUtils.copySSTablesAndIndexes;

public class LegacyIndexFileTest extends SAITester
{
    @Test
    public void test() throws Throwable
    {
//        DatabaseDescriptor.daemonInitialization();

        copySSTablesAndIndexes("aa");



        File srcDir = new File("build/test/cassandra/data/legacy-sai/aa");

        for (File file : srcDir.listFiles())
        {

        }

            Descriptor descriptor = Descriptor.fromFilename(srcDir.listFiles()[0]);

//        IndexComponents int_value = IndexComponents.create("int_index", descriptor, null);
//
//        int_value.validatePerSSTableComponents();
//        int_value.validatePerColumnComponents(false);
//        int_value.validatePerColumnComponentsChecksum(false);
//
//        IndexComponents text_value = IndexComponents.create("text_index", descriptor, null);
//
//        text_value.validatePerSSTableComponents();
//        text_value.validatePerColumnComponents(true);
//        text_value.validatePerColumnComponentsChecksum(true);
    }


}
