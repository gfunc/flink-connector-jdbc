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

package org.apache.flink.connector.jdbc.adb.table;

import org.apache.flink.connector.jdbc.adb.AdbTestBase;
import org.apache.flink.connector.jdbc.adb.database.catalog.AdbCatalog;
import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.runtime.utils.StreamTestSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;

/** ADB catalog read tests. */
public class AdbTableSourceITCase implements AdbTestBase, DatabaseTest {
    private static final String TEST_CATALOG_NAME = "adb_catalog";
    private static final String TEST_DB = "ods_crw";

    private AdbCatalog catalog;
    private TableEnvironment tEnv;

    @BeforeEach
    void beforeEach() {
        String jdbcUrl = getMetadata().getJdbcUrl();
        jdbcUrl = jdbcUrl.endsWith("/") ? jdbcUrl : jdbcUrl + "/";
        catalog =
                new AdbCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        TEST_CATALOG_NAME,
                        TEST_DB,
                        getMetadata().getUsername(),
                        getMetadata().getPassword(),
                        jdbcUrl.substring(0, jdbcUrl.lastIndexOf("/")));

        this.tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        // Use mysql catalog.
        tEnv.registerCatalog(TEST_CATALOG_NAME, catalog);
        tEnv.useCatalog(TEST_CATALOG_NAME);
    }

    @AfterEach
    void afterEach() {
        StreamTestSink.clear();
    }

    @Test
    void testRead() {
        String testTable = "ods_crw_all_coach_shop_oms_order";
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s limit 1;", testTable))
                                .execute()
                                .collect());

        assertThat(results).hasSize(1);
    }
}
