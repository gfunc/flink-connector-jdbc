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

package org.apache.flink.connector.jdbc.clickhouse.table;

import org.apache.flink.connector.jdbc.clickhouse.ClickHouseTestBase;
import org.apache.flink.connector.jdbc.clickhouse.database.dialect.ClickHouseDialect;
import org.apache.flink.connector.jdbc.core.table.sink.JdbcDynamicTableSinkITCase;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;

import static org.apache.flink.connector.jdbc.clickhouse.testutils.ClickHouseTableBuilder.dbType;
import static org.apache.flink.connector.jdbc.clickhouse.testutils.ClickHouseTableBuilder.field;
import static org.apache.flink.connector.jdbc.clickhouse.testutils.ClickHouseTableBuilder.pkField;
import static org.apache.flink.connector.jdbc.clickhouse.testutils.ClickHouseTableBuilder.tableRow;

/** The Table Sink ITCase for {@link ClickHouseDialect}. */
public class ClickHouseDynamicTableSinkITCase extends JdbcDynamicTableSinkITCase
        implements ClickHouseTestBase {

    @Override
    protected TableRow createUpsertOutputTable() {
        return tableRow(
                "dynamicSinkForUpsert",
                pkField("cnt", dbType("Int64").notNull(), DataTypes.BIGINT().notNull()),
                field("lencnt", dbType("Int64").notNull(), DataTypes.BIGINT().notNull()),
                pkField("cTag", dbType("Int").notNull(), DataTypes.INT().notNull()),
                field("ts", dbType("DateTime64(3, 'UTC')"), DataTypes.TIMESTAMP()));
    }

    @Override
    protected TableRow createAppendOutputTable() {
        return tableRow(
                "dynamicSinkForAppend",
                field("id", DataTypes.INT().notNull()),
                field("num", DataTypes.BIGINT().notNull()),
                field("ts", dbType("DateTime64(3, 'UTC')"), DataTypes.TIMESTAMP()));
    }

    protected TableRow createBatchOutputTable() {
        return tableRow(
                "dynamicSinkForBatch",
                field("NAME", DataTypes.VARCHAR(20).notNull()),
                field("SCORE", DataTypes.BIGINT().notNull()));
    }

    @Override
    protected TableRow createRealOutputTable() {
        return tableRow("REAL_TABLE", field("real_data", dbType("REAL"), DataTypes.FLOAT()));
    }

    @Override
    protected TableRow createCheckpointOutputTable() {
        return tableRow("checkpointTable", field("id", DataTypes.BIGINT().notNull()));
    }

    @Override
    protected TableRow createUserOutputTable() {
        return tableRow(
                "USER_TABLE",
                pkField("user_id", DataTypes.VARCHAR(20).notNull()),
                field("user_name", DataTypes.VARCHAR(20).notNull()),
                field("email", DataTypes.VARCHAR(255)),
                field("balance", DataTypes.DECIMAL(18, 2)),
                field("balance2", DataTypes.DECIMAL(18, 2)));
    }
}
