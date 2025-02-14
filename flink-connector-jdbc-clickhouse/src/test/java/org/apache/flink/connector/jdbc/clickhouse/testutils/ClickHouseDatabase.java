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

package org.apache.flink.connector.jdbc.clickhouse.testutils;

import org.apache.flink.connector.jdbc.testutils.DatabaseExtension;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseResource;
import org.apache.flink.connector.jdbc.testutils.resources.DockerResource;
import org.apache.flink.util.FlinkRuntimeException;

import org.testcontainers.clickhouse.ClickHouseContainer;

import java.util.TimeZone;

/** A ClickHouse database for testing. */
public class ClickHouseDatabase extends DatabaseExtension implements ClickHouseImages {
    static {
        // Set the default time zone to UTC
        // Cannot change ClickHouse container time zone, so we need to set the default time zone
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    private static final ClickHouseContainer CONTAINER =
            new ClickHouseContainer(CH_24)
                    .withUsername("clickhouse")
                    .withPassword("clickhouse")
                    .withDatabaseName("test")
                    .withEnv("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
                    .withUrlParam("use_server_time_zone", "false")
                    .withUrlParam("use_time_zone", TimeZone.getDefault().getID())
                    .withUrlParam("clickhouse.jdbc.v2", "true");

    private static ClickHouseMetadata metadata;

    public static ClickHouseMetadata getMetadata() {
        if (!CONTAINER.isRunning()) {
            throw new FlinkRuntimeException("Container is stopped.");
        }
        if (metadata == null) {
            metadata = new ClickHouseMetadata(CONTAINER);
        }
        return metadata;
    }

    @Override
    protected DatabaseMetadata getMetadataDB() {
        return getMetadata();
    }

    @Override
    protected DatabaseResource getResource() {
        return new DockerResource(CONTAINER);
    }
}
