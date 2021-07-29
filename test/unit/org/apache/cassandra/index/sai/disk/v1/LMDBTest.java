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

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import static java.lang.Long.BYTES;
import static java.lang.Long.MIN_VALUE;
import static java.nio.ByteBuffer.allocateDirect;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPFIXED;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.PutFlags.MDB_APPENDDUP;
import static org.lmdbjava.PutFlags.MDB_MULTIPLE;
import static org.lmdbjava.PutFlags.MDB_NODUPDATA;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.SeekOp.MDB_FIRST;
import static org.lmdbjava.SeekOp.MDB_GET_BOTH;

public class LMDBTest
{
    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    public static final int POSIX_MODE = 0664;

//    static ByteBuffer bb(final int value) {
//        final ByteBuffer bb = allocateDirect(Integer.BYTES);
//        bb.putInt(value).flip();
//        return bb;
//    }

    static ByteBuffer bb(final String value) {
        final ByteBuffer bb = allocateDirect(128);
        bb.put(value.getBytes(StandardCharsets.UTF_8)).flip();
        return bb;
    }

    @Test
    public void test() throws Exception
    {
        final File path = tmp.newFile();
        Env<ByteBuffer> env = create(PROXY_OPTIMAL)
              .setMapSize(1024*1024)
              .setMaxReaders(1)
              .setMaxDbs(1)
              .open(path, POSIX_MODE, MDB_NOSUBDIR);

        final Dbi<ByteBuffer> db = env.openDbi("test-db", MDB_CREATE);
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            try (Cursor<ByteBuffer> c = db.openCursor(txn)) {
                c.put(bb("aaaaaab"), bb("ccccrrrrrggg"));
                c.put(bb("rrerewrewrew"), bb("jjjjsdflsdfjsd"));
            }
            txn.commit();
        }
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            try (Cursor<ByteBuffer> c = db.openCursor(txn)) {
                while (c.next())
                {
                    final ByteBuffer key1 = c.key().duplicate();
                    final ByteBuffer val1 = c.val().duplicate();

                    System.out.println("key1=" + ByteBufferUtil.string(key1) + " val1=" + ByteBufferUtil.string(val1));
                }
            }
        }
    }
}
