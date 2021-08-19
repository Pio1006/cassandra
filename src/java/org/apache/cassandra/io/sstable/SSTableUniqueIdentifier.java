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

package org.apache.cassandra.io.sstable;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.cassandra.db.Directories;

/**
 * Represents a unique identfier in the sstable descriptor filename.
 * This ensures each sstable files uniqueness in the system.
 * <p>
 * A new iteration implementation must adhere to the following invariants:
 * - Must be roughly sortable (for determinism)
 * - String representation must *not* include the '-' character see {@link Descriptor#filenameSplitter}
 * <p>
 */
public interface SSTableUniqueIdentifier extends Comparable<SSTableUniqueIdentifier>
{
    /**
     * Creates a byte format of the identifier that can be parsed by
     * {@link Factory#fromBytes(ByteBuffer)}
     */
    ByteBuffer asBytes();

    /**
     * Creates a String format of the identifier that can be parsed by
     * {@link Factory#fromString(String)}
     * <p>
     * Must not contain any '-' character as it is used in the Descriptor
     * see {@link Descriptor#filenameSplitter}
     */
    String asString();

    public interface Builder<T extends SSTableUniqueIdentifier>
    {
        Supplier<T> generator(Stream<SSTableUniqueIdentifier> existingIdentifiers);

        T fromString(String token);

        T fromBytes(ByteBuffer bytes);
    }
}
