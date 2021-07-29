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

package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.util.Arrays;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.store.GrowableByteArrayDataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.packed.DirectWriter;

public class PrefixBytesWriter
{
    private final GrowableByteArrayDataOutput scratchOut = new GrowableByteArrayDataOutput(8 * 1024);
    private final GrowableByteArrayDataOutput prefixScratchOut = new GrowableByteArrayDataOutput(8 * 1024);
    private final GrowableByteArrayDataOutput lengthsScratchOut = new GrowableByteArrayDataOutput(8 * 1024);
    
    public void write(TermsIterator terms, IndexOutput out, int blockSize) throws IOException
    {
        scratchOut.reset();
        prefixScratchOut.reset();
        lengthsScratchOut.reset();

        final int[] lengths = new int[blockSize];
        final int[] prefixes = new int[blockSize];
        final long[] postings = new long[blockSize];
        int index = 0;

        final BytesRefBuilder builder = new BytesRefBuilder();
        final BytesRefBuilder prefixTermBuilder = new BytesRefBuilder();

        while (terms.hasNext())
        {
            final ByteComparable term = terms.next();
            int length = 0;
            final ByteSource byteSource = term.asComparableBytes(ByteComparable.Version.OSS41);

            builder.clear();

            // gather the term bytes from the byteSource
            while (true)
            {
                final int val = byteSource.next();
                if (val != ByteSource.END_OF_STREAM)
                {
                    ++length;
                    builder.append((byte)val);
                }
                else
                {
                    break;
                }
            }

            // System.out.println("term="+builder.get().utf8ToString());

            if (prefixTermBuilder.length() == 0)
            {
                assert index == 0;
                prefixTermBuilder.append(builder);
                prefixes[index] = 0;
                lengths[index] = builder.get().length;
            }
            else
            {
                final int prefix = StringHelper.bytesDifference(prefixTermBuilder.get(), builder.get());
                System.out.println("prefix="+prefixTermBuilder.get().utf8ToString()+" prefix="+prefix);
                prefixes[index] = prefix;
                lengths[index] = builder.get().length;
            }
            System.out.println("term="+builder.get().utf8ToString()+" prefix="+prefixes[index]+" length="+lengths[index]);

            int start = prefixes[index];
            int len = builder.get().length - prefixes[index];

            if (index == 0)
            {
                prefixes[index] = builder.get().length;
            }

            System.out.println("write index="+index+" start="+start+" len="+len);
            scratchOut.writeBytes(builder.get().bytes, start, len);

            final PostingList postingList = terms.postings();
            int postingIndex = 0;
            while (true)
            {
                final long rowid = postingList.nextPosting();

                if (rowid == PostingList.END_OF_STREAM) break;

                postings[postingIndex] = rowid;

                if (postingIndex > 0)
                {
                    lengths[index] = 0; // when the length is 0 the term hasn't changed
                }
                else
                {
                    lengths[index] = length;
                }
                postingIndex++;
                index++;
            }
            System.out.println("postings=" + Arrays.toString(Arrays.copyOf(postings, postingIndex)));
        }
        System.out.println("lengths=" + Arrays.toString(Arrays.copyOf(lengths, index))+" scratchOut.length="+scratchOut.getPosition());
        final int maxLength = Arrays.stream(lengths).max().getAsInt();
        LeafOrderMap.write(lengths, index, maxLength, lengthsScratchOut);
        final int maxPrefix = Arrays.stream(prefixes).max().getAsInt();
        System.out.println("prefixes=" + Arrays.toString(Arrays.copyOf(prefixes, index)));
        LeafOrderMap.write(prefixes, index, maxPrefix, prefixScratchOut);

        out.writeInt(index); // value count
        out.writeInt(lengthsScratchOut.getPosition());
        out.writeInt(prefixScratchOut.getPosition());
        out.writeByte((byte)DirectWriter.unsignedBitsRequired(maxLength));
        out.writeByte((byte)DirectWriter.unsignedBitsRequired(maxPrefix));
        out.writeBytes(lengthsScratchOut.getBytes(), 0, lengthsScratchOut.getPosition());
        out.writeBytes(prefixScratchOut.getBytes(), 0, prefixScratchOut.getPosition());
        System.out.println("write bytes file pointer="+out.getFilePointer());
        out.writeBytes(scratchOut.getBytes(), 0, scratchOut.getPosition());
    }
}
