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
package org.apache.cassandra.service;

import java.util.function.Function;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SnapshotCommand;
import org.apache.cassandra.db.Truncation;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.net.DroppedMessages;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.Verb.RequestResponse;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.Verb.AckedRequest;
import org.apache.cassandra.net.VerbGroup;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;

public class OperationsVerbs extends VerbGroup<OperationsVerbs.OperationsVersion>
{
    private static final Logger logger = LoggerFactory.getLogger(OperationsVerbs.class);

    public enum OperationsVersion implements Version<OperationsVersion>
    {
        OSS_30,
        DSE_60; // Don't send anything with a TruncateResponse (we use to send back the table truncated and a success
                // boolean but it was completly ignored on the receiving side. And 1) we don't need the table, we know it
                // and 2) errors are sent back as failure response now).

        public static <T> Versioned<OperationsVersion, T> versioned(Function<OperationsVersion, ? extends T> function)
        {
            return new Versioned<>(OperationsVersion.class, function);
        }
    }

    public RequestResponse<Truncation, TruncateResponse> TRUNCATE;
    public AckedRequest<SnapshotCommand> SNAPSHOT;
    public AckedRequest<EmptyPayload> REPLICATION_FINISHED;

    public OperationsVerbs(Verbs.Group id)
    {
        super(id, true, OperationsVersion.class);

        RegistrationHelper helper = helper();

        TRUNCATE = helper.requestResponse("TRUNCATE", Truncation.class, TruncateResponse.class)
                         .requestStage(Stage.MISC)
                         .droppedGroup(DroppedMessages.Group.TRUNCATE)
                         .timeout(DatabaseDescriptor::getTruncateRpcTimeout)
                         .syncHandler((from, t) ->
                                      {
                                          Tracing.trace("Applying truncation of {}.{}", t.keyspace, t.columnFamily);
                                          try
                                          {
                                              ColumnFamilyStore cfs = Keyspace.open(t.keyspace).getColumnFamilyStore(t.columnFamily);
                                              cfs.truncateBlocking();
                                              return new TruncateResponse(t.keyspace, t.columnFamily);
                                          }
                                          catch (Exception e)
                                          {
                                              logger.error("Error in truncation", e);
                                              FSError fsError = FSError.findNested(e);
                                              throw Throwables.propagate(fsError == null ? e : fsError);
                                          }
                                      });

        SNAPSHOT = helper.ackedRequest("SNAPSHOT", SnapshotCommand.class)
                         .requestStage(Stage.MISC)
                         .droppedGroup(DroppedMessages.Group.SNAPSHOT)
                         .timeout(DatabaseDescriptor::getRpcTimeout)
                         .syncHandler((from, command) ->
                                      {
                                          if (command.clearSnapshot)
                                              Keyspace.clearSnapshot(command.snapshotName, command.keyspace);
                                          else
                                              Keyspace.open(command.keyspace).getColumnFamilyStore(command.table).snapshot(command.snapshotName);
                                      });

        REPLICATION_FINISHED = helper.ackedRequest("REPLICATION_FINISHED", EmptyPayload.class)
                                     .requestStage(Stage.MISC)
                                     .droppedGroup(DroppedMessages.Group.OTHER)
                                     .timeout(DatabaseDescriptor::getRpcTimeout)
                                     .syncHandler((from, x) -> StorageService.instance.confirmReplication(from));
    }
}