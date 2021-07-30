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
import java.lang.invoke.MethodHandles;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.BKDReader;
import org.apache.cassandra.index.sai.metrics.MulticastQueryEventListeners;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.index.sai.disk.BKDQueries.bkdQueryFrom;

/**
 * Executes {@link Expression}s against the kd-tree for an individual index segment.
 */
public class KDTreeIndexSearcher extends IndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final BKDReader bkdReader;
    private final QueryEventListener.BKDIndexEventListener perColumnEventListener;

    KDTreeIndexSearcher(Segment segment, QueryEventListener.BKDIndexEventListener listener) throws IOException
    {
        super(segment);

        final long bkdPosition = metadata.getIndexRoot(IndexComponent.Type.KD_TREE);
        assert bkdPosition >= 0;
        final long postingsPosition = metadata.getIndexRoot(IndexComponent.Type.KD_TREE_POSTING_LISTS);
        assert postingsPosition >= 0;

        bkdReader = new BKDReader(indexFiles.get(IndexComponent.Type.KD_TREE).sharedCopy(),
                                  bkdPosition,
                                  indexFiles.get(IndexComponent.Type.KD_TREE_POSTING_LISTS).sharedCopy(),
                                  postingsPosition);
        perColumnEventListener = listener;

    }

    @Override
    public long indexFileCacheSize()
    {
        return bkdReader.memoryUsage();
    }

    @Override
    @SuppressWarnings("resource")
    public RangeIterator search(Expression exp, SSTableQueryContext context, boolean defer)
    {
        //TODO Fix logmessage
//        if (logger.isTraceEnabled())
//            logger.trace(indexComponents.logMessage("Searching on expression '{}'..."), exp);

        if (exp.getOp().isEqualityOrRange())
        {
            final BKDReader.IntersectVisitor query = bkdQueryFrom(exp, bkdReader.getNumDimensions(), bkdReader.getBytesPerDimension());
            QueryEventListener.BKDIndexEventListener listener = MulticastQueryEventListeners.of(context.queryContext, perColumnEventListener);

            PostingList postingList = defer ? new PostingList.DeferredPostingList(() -> bkdReader.intersect(query, listener, context.queryContext))
                                            : bkdReader.intersect(query, listener, context.queryContext);
            return toIterator(postingList, context, defer);
        }
        else
        {
            //TODO Fix logmessage
//            throw new IllegalArgumentException(indexComponents.logMessage(indexComponents.logMessage("Unsupported expression during index query: " + exp)));
            throw new IllegalArgumentException();
        }
    }

    public static int openPerIndexFiles()
    {
        return BKDReader.openPerIndexFiles();
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          //TODO Fix string
//                          .add("indexComponents", indexComponents)
//                          .add("diskSize", FBUtilities.prettyPrintMemory(indexComponents.sizeOfPerColumnComponents()))
                          .add("count", bkdReader.getPointCount())
                          .add("numDimensions", bkdReader.getNumDimensions())
                          .add("bytesPerDimension", bkdReader.getBytesPerDimension())
                          .toString();
    }

    @Override
    public void close()
    {
        bkdReader.close();
    }
}
