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
package org.apache.cassandra.transport.messages;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.reactivex.Observable;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Connection;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.transport.ServerConnection;

public class RegisterMessage extends Message.Request
{
    public static final Message.Codec<RegisterMessage> codec = new Message.Codec<RegisterMessage>()
    {
        public RegisterMessage decode(ByteBuf body, int version)
        {
            int length = body.readUnsignedShort();
            List<Event.Type> eventTypes = new ArrayList<>(length);
            for (int i = 0; i < length; ++i)
                eventTypes.add(CBUtil.readEnumValue(Event.Type.class, body));
            return new RegisterMessage(eventTypes);
        }

        public void encode(RegisterMessage msg, ByteBuf dest, int version)
        {
            dest.writeShort(msg.eventTypes.size());
            for (Event.Type type : msg.eventTypes)
                CBUtil.writeEnumValue(type, dest);
        }

        public int encodedSize(RegisterMessage msg, int version)
        {
            int size = 2;
            for (Event.Type type : msg.eventTypes)
                size += CBUtil.sizeOfEnumValue(type);
            return size;
        }
    };

    public final List<Event.Type> eventTypes;

    public RegisterMessage(List<Event.Type> eventTypes)
    {
        super(Message.Type.REGISTER);
        this.eventTypes = eventTypes;
    }

    public Observable<Response> execute(QueryState state, long queryStartNanoTime)
    {
        assert connection instanceof ServerConnection;
        Connection.Tracker tracker = connection.getTracker();
        assert tracker instanceof Server.ConnectionTracker;
        for (Event.Type type : eventTypes)
        {
            if (type.minimumVersion > connection.getVersion())
                throw new ProtocolException("Event " + type.name() + " not valid for protocol version " + connection.getVersion());
            ((Server.ConnectionTracker) tracker).register(type, connection().channel());
        }
        return Observable.just(new ReadyMessage());
    }

    @Override
    public String toString()
    {
        return "REGISTER " + eventTypes;
    }
}
