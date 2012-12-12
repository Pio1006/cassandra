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
package org.apache.cassandra.auth;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.thrift.*;

public class Auth
{
    private static final Logger logger = LoggerFactory.getLogger(Auth.class);

    public static final String DEFAULT_SUPERUSER_NAME = "cassandra";

    // 'system_auth' in 1.2.
    public static final String AUTH_KS = "dse_auth";
    public static final String USERS_CF = "users";

    /**
     * Checks if the username is stored in AUTH_KS.USERS_CF.
     *
     * @param username Username to query.
     * @return whether or not Cassandra knows about the user.
     */
    public static boolean isExistingUser(String username)
    {
        String query = String.format("SELECT * FROM %s.%s WHERE name = '%s'", AUTH_KS, USERS_CF, escape(username));
        try
        {
            return !QueryProcessor.processInternal(query).type.equals(CqlResultType.VOID);
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e);
        }
        catch (TimedOutException e)
        {
            throw new RuntimeException(e);
        }
        catch (UnavailableException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Checks if the user is a known superuser.
     *
     * @param username Username to query.
     * @return true is the user is a superuser, false if they aren't or don't exist at all.
     */
    public static boolean isSuperuser(String username)
    {
        String query = String.format("SELECT super FROM %s.%s WHERE name = '%s'", AUTH_KS, USERS_CF, escape(username));
        try
        {
            CqlResult result = QueryProcessor.processInternal(query);
            return !result.type.equals(CqlResultType.VOID) && new UntypedResultSet(result.rows).one().getBoolean("super");
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e);
        }
        catch (TimedOutException e)
        {
            throw new RuntimeException(e);
        }
        catch (UnavailableException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Inserts the user into AUTH_KS.USERS_CF (or overwrites their superuser status as a result of an ALTER USER query).
     *
     * @param username Username to insert.
     * @param isSuper User's new status.
     */
    public static void insertUser(String username, boolean isSuper)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        QueryProcessor.processInternal(String.format("INSERT INTO %s.%s (name, super) VALUES ('%s', '%s')",
                                                     AUTH_KS,
                                                     USERS_CF,
                                                     escape(username),
                                                     isSuper));
    }

    /**
     * Deletes the user from AUTH_KS.USERS_CF.
     *
     * @param username Username to delete.
     */
    public static void deleteUser(String username)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        QueryProcessor.processInternal(String.format("DELETE FROM %s.%s WHERE name = '%s'",
                                                     AUTH_KS,
                                                     USERS_CF,
                                                     escape(username)));
    }

    /**
     * Sets up Authenticator and Authorizer.
     */
    public static void setup()
    {
        authenticator().setup();
        authorizer().setup();
    }

    /**
     * Sets up default superuser.
     */
    public static void setupSuperuser()
    {
        try
        {
            // insert a default superuser if AUTH_KS.USERS_CF is empty.
            if(QueryProcessor.processInternal(String.format("SELECT * FROM %s.%s", AUTH_KS, USERS_CF)).type.equals(CqlResultType.VOID))
                insertUser(DEFAULT_SUPERUSER_NAME, true);
        }
        catch (Exception e)
        {
            logger.warn("Skipping default superuser setup: some nodes are not ready");
        }
    }

    // we only worry about one character ('). Make sure it's properly escaped.
    private static String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    private static IAuthenticator authenticator()
    {
        return DatabaseDescriptor.getAuthenticator();
    }

    private static IAuthorizer authorizer()
    {
        return DatabaseDescriptor.getAuthorizer();
    }
}
