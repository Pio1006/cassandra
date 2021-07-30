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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.io.CryptoUtils;
import org.apache.cassandra.index.sai.disk.io.RAMIndexOutput;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

/**
 * Multiple {@link SegmentMetadata} are stored in {@link IndexComponent.Type#META} file, each corresponds to an on-disk
 * index segment.
 */
public class SegmentMetadata implements Comparable<SegmentMetadata>
{
    private static final String NAME = "SegmentMetadata";

    /**
     * Used to retrieve sstableRowId which equals to offset plus segmentRowId.
     */
    public final long segmentRowIdOffset;

    /**
     * Min and max sstable rowId in current segment.
     *
     * For index generated by compaction, minSSTableRowId is the same as segmentRowIdOffset.
     * But for flush, segmentRowIdOffset is taken from previous segment's maxSSTableRowId.
     */
    public final long minSSTableRowId;
    public final long maxSSTableRowId;

    /**
     * number of indexed rows (aka. pair of term and segmentRowId) in current segment
     */
    public final long numRows;

    /**
     * Ordered by their token position in current segment
     */
    public final DecoratedKey minKey;
    public final DecoratedKey maxKey;

    /**
     * Minimum and maximum indexed column value ordered by its {@link org.apache.cassandra.db.marshal.AbstractType}.
     */
    public final ByteBuffer minTerm, maxTerm;

    /**
     * Root, offset, length for each index structure in the segment.
     *
     * Note: postings block offsets are stored in terms dictionary, no need to worry about its root.
     */
    public final ComponentMetadataMap componentMetadatas;

    SegmentMetadata(long segmentRowIdOffset,
                    long numRows,
                    long minSSTableRowId,
                    long maxSSTableRowId,
                    DecoratedKey minKey,
                    DecoratedKey maxKey,
                    ByteBuffer minTerm,
                    ByteBuffer maxTerm,
                    ComponentMetadataMap componentMetadatas)
    {
        assert numRows < Integer.MAX_VALUE;
        Objects.requireNonNull(minKey);
        Objects.requireNonNull(maxKey);
        Objects.requireNonNull(minTerm);
        Objects.requireNonNull(maxTerm);

        this.segmentRowIdOffset = segmentRowIdOffset;
        this.minSSTableRowId = minSSTableRowId;
        this.maxSSTableRowId = maxSSTableRowId;
        this.numRows = numRows;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.minTerm = minTerm;
        this.maxTerm = maxTerm;
        this.componentMetadatas = componentMetadatas;
    }

    private static final Logger logger = LoggerFactory.getLogger(SegmentMetadata.class);

    @SuppressWarnings("resource")
    private SegmentMetadata(IndexInput input, ICompressor compressor) throws IOException
    {
        this.segmentRowIdOffset = input.readLong();

        numRows = input.readLong();
        minSSTableRowId = input.readLong();
        maxSSTableRowId = input.readLong();
        minKey = DatabaseDescriptor.getPartitioner().decorateKey(readBytes(input));
        maxKey = DatabaseDescriptor.getPartitioner().decorateKey(readBytes(input));

        if (compressor != null)
        {
            IndexInput cryptoIn = CryptoUtils.uncompress(input, compressor);

            assert cryptoIn.length() > 0;

            minTerm = readBytes(cryptoIn);
            maxTerm = readBytes(cryptoIn);
        }
        else
        {
            minTerm = readBytes(input);
            maxTerm = readBytes(input);
        }
        componentMetadatas = new ComponentMetadataMap(input);
    }

    @SuppressWarnings("resource")
    public static List<SegmentMetadata> load(MetadataSource source, ICompressor compressor) throws IOException
    {
        IndexInput input = source.get(NAME);

        int segmentCount = input.readVInt();

        List<SegmentMetadata> segmentMetadata = new ArrayList<>(segmentCount);

        for (int i = 0; i < segmentCount; i++)
        {
            segmentMetadata.add(new SegmentMetadata(input, compressor));
        }

        return segmentMetadata;
    }

    /**
     * Writes disk metadata for the given segment list.
     */
    @SuppressWarnings("resource")
    public static void write(MetadataWriter writer, List<SegmentMetadata> segments, ICompressor compressor) throws IOException
    {
        try (IndexOutput output = writer.builder(NAME))
        {
            output.writeVInt(segments.size());

            for (SegmentMetadata metadata : segments)
            {
                output.writeLong(metadata.segmentRowIdOffset);
                output.writeLong(metadata.numRows);
                output.writeLong(metadata.minSSTableRowId);
                output.writeLong(metadata.maxSSTableRowId);

                if (compressor != null)
                {
                    Stream.of(metadata.minKey.getKey(), metadata.maxKey.getKey()).forEach(bb -> writeBytes(bb, output));

                    RAMIndexOutput out = new RAMIndexOutput("");
                    writeBytes(metadata.minTerm, out);
                    writeBytes(metadata.maxTerm, out);

                    CryptoUtils.compress(new BytesRef(out.getBytes(), 0, (int)out.getFilePointer()), output, compressor);
                }
                else
                {
                    Stream.of(metadata.minKey.getKey(), metadata.maxKey.getKey(), metadata.minTerm, metadata.maxTerm).forEach(bb -> writeBytes(bb, output));
                }

                metadata.componentMetadatas.write(output);
            }
        }
    }

    @Override
    public int compareTo(SegmentMetadata other)
    {
        return Long.compare(this.segmentRowIdOffset, other.segmentRowIdOffset);
    }

    @Override
    public String toString()
    {
        return "SegmentMetadata{" +
               "segmentRowIdOffset=" + segmentRowIdOffset +
               ", minSSTableRowId=" + minSSTableRowId +
               ", maxSSTableRowId=" + maxSSTableRowId +
               ", numRows=" + numRows +
               ", componentMetadatas=" + componentMetadatas +
               '}';
    }

    private static ByteBuffer readBytes(IndexInput input) throws IOException
    {
        int len = input.readVInt();
        byte[] bytes = new byte[len];
        input.readBytes(bytes, 0, len);
        return ByteBuffer.wrap(bytes);
    }

    private static void writeBytes(ByteBuffer buf, IndexOutput out)
    {
        try
        {
            byte[] bytes = ByteBufferUtil.getArray(buf);
            out.writeVInt(bytes.length);
            out.writeBytes(bytes, 0, bytes.length);
        }
        catch (IOException ioe)
        {
            throw new RuntimeException(ioe);
        }
    }

    long getIndexRoot(IndexComponent.Type type)
    {
        return componentMetadatas.get(type).root;
    }

    public long getIndexOffset(IndexComponent component)
    {
        return componentMetadatas.get(component.type).offset;
    }

    public long getIndexLength(IndexComponent component)
    {
        return componentMetadatas.get(component.type).length;
    }

    public static class ComponentMetadataMap
    {
        private final Map<IndexComponent.Type, ComponentMetadata> metas = new HashMap<>();

        ComponentMetadataMap(IndexInput input) throws IOException
        {
            int size = input.readInt();

            for (int i = 0; i < size; i++)
            {
                metas.put(IndexComponent.Type.valueOf(input.readString()), new ComponentMetadata(input));
            }
        }

        public ComponentMetadataMap()
        {
        }

        public void put(IndexComponent.Type type, long root, long offset, long length)
        {
            metas.put(type, new ComponentMetadata(root, offset, length));
        }

        public void put(IndexComponent.Type type, long root, long offset, long length, Map<String, String> additionalMap)
        {
            metas.put(type, new ComponentMetadata(root, offset, length, additionalMap));
        }

        private void write(IndexOutput output) throws IOException
        {
            output.writeInt(metas.size());

            for (Map.Entry<IndexComponent.Type, ComponentMetadata> entry : metas.entrySet())
            {
                output.writeString(entry.getKey().name());
                entry.getValue().write(output);
            }
        }

        public ComponentMetadata get(IndexComponent.Type type)
        {
            if (!metas.containsKey(type))
                throw new IllegalArgumentException(type + " ComponentMetadata not found");

            return metas.get(type);
        }

        public Map<String, Map<String, String>> asMap()
        {
            Map<String, Map<String, String>> metaAttributes = new HashMap<>();

            for (Map.Entry<IndexComponent.Type, ComponentMetadata> entry : metas.entrySet())
            {
                String name = entry.getKey().name();
                ComponentMetadata metadata = entry.getValue();

                Map<String, String> componentAttributes = metadata.asMap();

                assert !metaAttributes.containsKey(name) : "Found duplicate index type: " + name;
                metaAttributes.put(name, componentAttributes);
            }

            return metaAttributes;
        }

        @Override
        public String toString()
        {
            return "ComponentMetadataMap{" +
                   "metas=" + metas +
                   '}';
        }

        public double indexSize()
        {
            return metas.values().stream().mapToLong(meta -> meta.length).sum();
        }
    }

    public static class ComponentMetadata
    {
        public static final String ROOT = "Root";
        public static final String OFFSET = "Offset";
        public static final String LENGTH = "Length";

        public final long root;
        public final long offset;
        public final long length;
        public final Map<String,String> attributes;

        ComponentMetadata(long root, long offset, long length)
        {
            this.root = root;
            this.offset = offset;
            this.length = length;
            this.attributes = Collections.emptyMap();
        }

        ComponentMetadata(long root, long offset, long length, Map<String, String> attributes)
        {
            this.root = root;
            this.offset = offset;
            this.length = length;
            this.attributes = attributes;
        }

        ComponentMetadata(IndexInput input) throws IOException
        {
            this.root = input.readLong();
            this.offset = input.readLong();
            this.length = input.readLong();
            int size = input.readInt();

            attributes = new HashMap<>(size);
            for (int x=0; x < size; x++)
            {
                String key = input.readString();
                String value = input.readString();

                attributes.put(key, value);
            }
        }

        public void write(IndexOutput output) throws IOException
        {
            output.writeLong(root);
            output.writeLong(offset);
            output.writeLong(length);

            output.writeInt(attributes.size());
            for (Map.Entry<String,String> entry : attributes.entrySet())
            {
                output.writeString(entry.getKey());
                output.writeString(entry.getValue());
            }
        }

        @Override
        public String toString()
        {
            return String.format("ComponentMetadata{root=%d, offset=%d, length=%d, attributes=%s}", root, offset, length, attributes.toString());
        }

        public Map<String, String> asMap()
        {
            return ImmutableMap.<String, String>builder().putAll(attributes).put(OFFSET, Long.toString(offset)).put(LENGTH, Long.toString(length)).put(ROOT, Long.toString(root)).build();
        }
    }
}
