/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.transform;

import io.reactivex.Observable;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.reactivestreams.Subscription;

final class UnfilteredRows extends BaseRows<Unfiltered, UnfilteredRowIterator> implements UnfilteredRowIterator
{
    private DeletionTime partitionLevelDeletion;

    public UnfilteredRows(UnfilteredRowIterator input)
    {
        super(input);
        partitionLevelDeletion = input.partitionLevelDeletion();
    }

    @Override
    void add(Transformation add)
    {
        super.add(add);
        partitionLevelDeletion = add.applyToDeletion(partitionLevelDeletion);
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public EncodingStats stats()
    {
        return input.stats();
    }

    @Override
    public boolean isEmpty()
    {
        return staticRow().isEmpty() && partitionLevelDeletion().isLive() && !hasNext();
    }

    public Observable<Unfiltered> asObservable()
    {
        return Observable.create(subscriber -> {
            subscriber.onSubscribe(new Subscription()
            {
                public void request(long l)
                {

                }

                public void cancel()
                {

                }
            });
            while(hasNext())
                subscriber.onNext(next());

            subscriber.onComplete();
        });
    }
}
