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

package org.apache.cassandra.index.sai.disk.io;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Throwables;

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.lucene.store.IndexInput;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TrackingIndexFileUtils extends IndexFileUtils
{
    private final Map<TrackingIndexInput, String> openInputs = Collections.synchronizedMap(new HashMap<>());

    public TrackingIndexFileUtils(SequentialWriterOption writerOption)
    {
        setWriterOptions(writerOption);
    }

    @Override
    public IndexInput openInput(FileHandle handle)
    {
        TrackingIndexInput input = new TrackingIndexInput(super.openInput(handle));
        openInputs.put(input, Throwables.getStackTraceAsString(new RuntimeException("Input created")));
        return input;
    }

    public Map<IndexInput, String> getOpenInputs()
    {
        return new HashMap<>(openInputs);
    }

    public static void reset()
    {
        setWriterOptions(IndexFileUtils.defaultWriterOption);
    }

    public class TrackingIndexInput extends FilterIndexInput
    {
        TrackingIndexInput(IndexInput delegate)
        {
            super(delegate);
        }

        @Override
        public void close() throws IOException
        {
            super.close();
            final String creationStackTrace = openInputs.remove(this);
            assertNotNull("Closed unregistered input: " + this, creationStackTrace);
        }
    }

    public static void setWriterOptions(SequentialWriterOption writerOptions)
    {
        try
        {
            Field instance = IndexFileUtils.class.getDeclaredField("instance");
            instance.setAccessible(true);
            Object instanceValue = instance.get(null);
            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            //        modifiersField.setInt(selectivity, selectivity.getModifiers() & ~Modifier.FINAL);
            //        selectivity.set(null, selectivityLimit);
        }
        catch (Throwable e)
        {
            fail();
        }
    }
}
