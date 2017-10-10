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
package org.apache.cassandra.io.util;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.RateLimiter;

/**
 * Rebufferer wrapper that applies rate limiting.
 *
 * Instantiated once per RandomAccessReader, thread-unsafe.
 * The instances reuse themselves as the BufferHolder to avoid having to return a new object for each rebuffer call.
 */
public class LimitingRebufferer extends WrappingRebufferer
{
    private final RateLimiter limiter;
    private final int limitQuant;

    LimitingRebufferer(Rebufferer wrapped, RateLimiter limiter, int limitQuant)
    {
        super(wrapped);
        this.limiter = limiter;
        this.limitQuant = limitQuant;
    }

    @Override
    public BufferHolder rebuffer(long position, ReaderConstraint rc)
    {
        WrappingBufferHolder ret = (WrappingBufferHolder)super.rebuffer(position, rc);

        int posInBuffer = Ints.checkedCast(position - ret.offset());
        int remaining = ret.limit() - posInBuffer;
        if (remaining == 0)
            return ret;

        if (remaining > limitQuant)
        {
            ret.limit(posInBuffer + limitQuant); // certainly below current limit
            remaining = limitQuant;
        }
        limiter.acquire(remaining);
        return ret;
    }

    @Override
    protected String paramsToString()
    {
        return limiter.toString();
    }
}
