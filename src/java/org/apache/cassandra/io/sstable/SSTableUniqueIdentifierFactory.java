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

public class SSTableUniqueIdentifierFactory
{
    public static final SSTableUniqueIdentifierFactory instance = new SSTableUniqueIdentifierFactory();

    /**
     * Converts a string serialized identifier back to a UID
     * <p>
     * {@link SSTableUniqueIdentifier#asString()}
     * <p>
     * A malformed value will throw a unchecked exception
     */
    SSTableUniqueIdentifier fromString(String str)
    {
        SSTableUniqueIdentifier.Builder<?> builder = str.length() == ULIDBasedSSTableUniqueIdentifier.STRING_LEN
                                                     ? ULIDBasedSSTableUniqueIdentifier.Builder.instance
                                                     : SequenceBasedSSTableUniqueIdentifier.Builder.instance;
        return builder.fromString(str);
    }

    /**
     * Converts a byte serialized identifier back to a UID
     * <p>
     * {@link SSTableUniqueIdentifier#asBytes()}
     * <p>
     * A malformed value will throw an unchecked exception
     */
    SSTableUniqueIdentifier fromBytes(ByteBuffer bytes)
    {
        SSTableUniqueIdentifier.Builder<?> builder = bytes.remaining() == ULIDBasedSSTableUniqueIdentifier.BYTES_LEN
                                                     ? ULIDBasedSSTableUniqueIdentifier.Builder.instance
                                                     : SequenceBasedSSTableUniqueIdentifier.Builder.instance;
        return builder.fromBytes(bytes);
    }

    public SSTableUniqueIdentifier.Builder<? extends SSTableUniqueIdentifier> defaultBuilder()
    {
        return ULIDBasedSSTableUniqueIdentifier.Builder.instance;
    }
}
