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

package org.apache.flink.connector.jdbc.clickhouse.database.catalog;

import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.JdbcITCaseBase;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.clickhouse.testutils.ClickHouseTableBuilder.dbType;
import static org.apache.flink.connector.jdbc.clickhouse.testutils.ClickHouseTableBuilder.field;
import static org.apache.flink.connector.jdbc.clickhouse.testutils.ClickHouseTableBuilder.pkField;
import static org.apache.flink.connector.jdbc.clickhouse.testutils.ClickHouseTableBuilder.tableRow;
import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test base for {@link ClickHouseCatalog}. */
abstract class ClickHouseCatalogTestBase implements JdbcITCaseBase, DatabaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseCatalogTestBase.class);

    private static final String TEST_CATALOG_NAME = "clickhouse_catalog";
    private static final String TEST_DB = "test";
    private static final String TEST_DB2 = "test2";

    private static final TableRow TABLE_ALL_TYPES = createTableAllTypeTable("t_all_types");
    private static final TableRow TABLE_ALL_TYPES_SINK =
            createTableAllTypeTable("t_all_types_sink");
    private static final TableRow TABLE_GROUPED_BY_SINK = createGroupedTable("t_grouped_by_sink");
    private static final TableRow TABLE_PK = createGroupedTable("t_pk");
    private static final TableRow TABLE_PK2 =
            tableRow(
                    "t_pk",
                    pkField("pid", dbType("Int64").notNull(), DataTypes.BIGINT()),
                    field("col_varchar", dbType("String"), DataTypes.STRING()));

    private static final List<Row> TABLE_ALL_TYPES_ROWS =
            Arrays.asList(
                    Row.ofKind(
                            RowKind.INSERT,
                            1L,
                            -1L,
                            false,
                            Date.valueOf("2021-08-04").toLocalDate(),
                            Date.valueOf("1900-03-12").toLocalDate(),
                            Timestamp.valueOf("2021-08-04 01:54:16").toLocalDateTime(),
                            Timestamp.valueOf("2021-08-04 01:54:16").toLocalDateTime(),
                            Timestamp.valueOf("2021-08-04 01:54:16").toLocalDateTime(),
                            Timestamp.valueOf("2021-08-04 01:54:16").toLocalDateTime(),
                            "hello",
                            "world",
                            "hello5",
                            Byte.valueOf("127"),
                            Short.valueOf("255"),
                            Short.valueOf("-32760"),
                            65535,
                            -2147483647,
                            4294967295L,
                            -9223372036854775808L,
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            new BigDecimal(1234567812345678d, new MathContext(16)).setScale(0),
                            new BigDecimal(1.2345678E23d, new MathContext(8)).setScale(0),
                            new BigDecimal(1.2345678E23d, new MathContext(8)).setScale(0),
                            new BigDecimal(1.2345678E37d, new MathContext(8)).setScale(0),
                            new BigDecimal(1.2345678E37d, new MathContext(8)).setScale(0),
                            new BigDecimal(1.36d, new MathContext(3)).setScale(2),
                            new BigDecimal(1.23d, new MathContext(3)).setScale(2),
                            new BigDecimal(1.23E2d, new MathContext(3)).setScale(8),
                            new BigDecimal(1.2345678E23d, new MathContext(8)).setScale(0),
                            new BigDecimal(1.2345678E37d, new MathContext(8)).setScale(0),
                            2.123456F,
                            1.79769E+40D,
                            "127.0.0.1",
                            "4c00:421f:f69b:fbae:4df2:43e8:6ba5:868b",
                            "1aa114b9-96d1-4ca2-8ff1-7db4a8a6dfaa",
                            //                            "TBD",
                            //                            "TBD",
                            //                            "TBD",
                            //                            "TBD",
                            //                            "{\"a\":\"b\", \"c\": \"d\"}",
                            //                            "TBD",
                            "teststring1",
                            new String[] {"string1", "string2"},
                            new String[] {"string1", "string2", null},
                            new LinkedHashMap<String, String>() {
                                {
                                    put("k1", "v1");
                                    put("k2", "v2");
                                }
                            },
                            Row.ofKind(RowKind.INSERT, 1L, "tuple1"),
                            Row.ofKind(
                                    RowKind.INSERT,
                                    new Long[] {11L, 13L},
                                    new LinkedHashMap<String, String>() {
                                        {
                                            put("k1", "v1");
                                            put("k2", "v2");
                                        }
                                    })),
                    Row.ofKind(
                            RowKind.INSERT,
                            2L,
                            -1L,
                            true,
                            Date.valueOf("2021-08-04").toLocalDate(),
                            Date.valueOf("1900-03-12").toLocalDate(),
                            Timestamp.valueOf("2021-08-04 01:54:16").toLocalDateTime(),
                            Timestamp.valueOf("2021-08-04 01:54:16").toLocalDateTime(),
                            Timestamp.valueOf("2021-08-04 01:54:16").toLocalDateTime(),
                            Timestamp.valueOf("2021-08-04 01:54:16").toLocalDateTime(),
                            "world",
                            "hello",
                            "world5",
                            Byte.valueOf("126"),
                            Short.valueOf("255"),
                            Short.valueOf("32760"),
                            65535,
                            -2147483647,
                            4294967295L,
                            -9223372036854775808L,
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            //                            Timestamp.valueOf("2021-08-04
                            // 01:54:16").toLocalDateTime().getYear(),
                            new BigDecimal(1234567812345678d, new MathContext(16)).setScale(0),
                            new BigDecimal(1.2345678E23d, new MathContext(8)).setScale(0),
                            new BigDecimal(1.2345678E23d, new MathContext(8)).setScale(0),
                            new BigDecimal(1.2345678E37d, new MathContext(8)).setScale(0),
                            new BigDecimal(1.2345678E37d, new MathContext(8)).setScale(0),
                            new BigDecimal(1.29d, new MathContext(3)).setScale(2),
                            new BigDecimal(1.23d, new MathContext(3)).setScale(2),
                            new BigDecimal(1.23E2d, new MathContext(3)).setScale(8),
                            new BigDecimal(1.2345678E23d, new MathContext(8)).setScale(0),
                            new BigDecimal(1.2345678E37d, new MathContext(8)).setScale(0),
                            2.123456F,
                            1.79769E+40D,
                            "192.168.1.1",
                            "a619:2cec:57bd:2f69:15b8:7fe0:6740:77e1",
                            "c2cb5330-d402-4ca8-b9db-9258538d8e53",
                            //                            "TBD",
                            //                            "TBD",
                            //                            "TBD",
                            //                            "TBD",
                            //                            "{\"x\":\"y\", \"q\": \"p\"}",
                            //                            "TBD",
                            "teststring2",
                            new String[] {"string3", "string4"},
                            new String[] {"string5", null},
                            new LinkedHashMap<String, String>() {
                                {
                                    put("k3", "v3");
                                    put("k4", "v4");
                                }
                            },
                            Row.ofKind(RowKind.INSERT, 2L, "tuple2"),
                            Row.ofKind(
                                    RowKind.INSERT,
                                    new Long[] {12L, 14L},
                                    new LinkedHashMap<String, String>() {
                                        {
                                            put("k3", "v3");
                                            put("k4", "v4");
                                        }
                                    })));
    private ClickHouseCatalog catalog;
    private TableEnvironment tEnv;

    @Override
    public List<TableManaged> getManagedTables() {
        return Arrays.asList(
                TABLE_ALL_TYPES, TABLE_ALL_TYPES_SINK, TABLE_GROUPED_BY_SINK, TABLE_PK);
    }

    private static TableRow createTableAllTypeTable(String tableName) {
        return tableRow(
                tableName,
                pkField("pid", dbType("Int64").notNull(), DataTypes.BIGINT().notNull()),
                field("col_bigint", dbType("Int64"), DataTypes.BIGINT()),
                field("col_bool", dbType("Bool"), DataTypes.BOOLEAN()),
                field("col_date", dbType("Date"), DataTypes.DATE()),
                field("col_date32", dbType("Date32"), DataTypes.DATE()),
                field("col_datetime", dbType("DateTime"), DataTypes.TIMESTAMP(9)),
                field("col_datetime32", dbType("DateTime32"), DataTypes.TIMESTAMP(9)),
                field("col_datetime64", dbType("DateTime64"), DataTypes.TIMESTAMP(9)),
                field("col_datetime64wTz", dbType("DateTime64(3, 'UTC')"), DataTypes.TIMESTAMP(9)),
                field(
                        "col_enum8",
                        dbType("Enum8('hello' = 1, 'world' = 2)").notNull(),
                        DataTypes.STRING().notNull()),
                field(
                        "col_enum16",
                        dbType("Enum16('hello' = 128, 'world' = 228)").notNull(),
                        DataTypes.STRING().notNull()),
                field(
                        "col_fixedstring",
                        dbType("FixedString(6)").notNull(),
                        DataTypes.VARCHAR(6).notNull()),
                field("col_int8", dbType("Int8"), DataTypes.TINYINT()),
                field("col_uint8", dbType("UInt8"), DataTypes.SMALLINT()),
                field("col_int16", dbType("Int16"), DataTypes.SMALLINT()),
                field("col_uint16", dbType("UInt16"), DataTypes.INT()),
                field("col_int32", dbType("Int32"), DataTypes.INT()),
                field("col_uint32", dbType("UInt32"), DataTypes.BIGINT()),
                field("col_int64", dbType("Int64"), DataTypes.BIGINT()),
                //                field("col_intervalyear", dbType("IntervalYear"),
                // DataTypes.BIGINT()),
                //                field("col_intervalquarter", dbType("IntervalQuarter"),
                // DataTypes.BIGINT()),
                //                field("col_intervalmonth", dbType("IntervalMonth"),
                // DataTypes.BIGINT()),
                //                field("col_intervalweek", dbType("IntervalWeek"),
                // DataTypes.BIGINT()),
                //                field("col_intervalday", dbType("IntervalDay"),
                // DataTypes.BIGINT()),
                //                field("col_intervalhour", dbType("IntervalHour"),
                // DataTypes.BIGINT()),
                //                field("col_intervalminute", dbType("IntervalMinute"),
                // DataTypes.BIGINT()),
                //                field("col_intervalsecond", dbType("IntervalSecond"),
                // DataTypes.BIGINT()),
                //                field("col_intervalmicrosecond", dbType("IntervalMicrosecond"),
                // DataTypes.BIGINT()),
                //                field("col_intervalmillisecond", dbType("IntervalMillisecond"),
                // DataTypes.BIGINT()),
                //                field("col_intervalnanosecond", dbType("IntervalNanosecond"),
                // DataTypes.BIGINT()),
                field("col_uint64", dbType("UInt64"), DataTypes.DECIMAL(20, 0)),
                field("col_int128", dbType("Int128"), DataTypes.DECIMAL(38, 0)),
                field("col_uint128", dbType("UInt128"), DataTypes.DECIMAL(38, 0)),
                field("col_int256", dbType("Int256"), DataTypes.DECIMAL(38, 0)),
                field("col_uint256", dbType("UInt256"), DataTypes.DECIMAL(38, 0)),
                field("col_decimal", dbType("Decimal(4,2)"), DataTypes.DECIMAL(4, 2)),
                field("col_decimal32", dbType("Decimal32(2)"), DataTypes.DECIMAL(9, 2)),
                field("col_decimal64", dbType("Decimal64(8)"), DataTypes.DECIMAL(18, 8)),
                field("col_decimal128", dbType("Decimal128(12)"), DataTypes.DECIMAL(38, 0)),
                field("col_decimal256", dbType("Decimal256(32)"), DataTypes.DECIMAL(38, 0)),
                field("col_float32", dbType("Float32"), DataTypes.FLOAT()),
                field("col_float64", dbType("Float64"), DataTypes.DOUBLE()),
                field("col_ipv4", dbType("IPv4"), DataTypes.VARCHAR(10)),
                field("col_ipv6", dbType("IPv6"), DataTypes.VARCHAR(39)),
                field("col_uuid", dbType("UUID"), DataTypes.VARCHAR(69)),
                //                field("col_point", dbType("Point"), DataTypes.STRING()),
                //                field("col_ring", dbType("Ring"), DataTypes.STRING()),
                //                field("col_polygon", dbType("Polygon"), DataTypes.STRING()),
                //                field("col_multipolygon", dbType("MultiPolygon"),
                // DataTypes.STRING()),
                //                field("col_json", dbType("JSON").notNull(), DataTypes.STRING()),
                //                field("col_object", dbType("Object"), DataTypes.STRING()),
                field("col_string", dbType("String"), DataTypes.STRING()),
                field(
                        "col_array",
                        dbType("Array(String)").notNull(),
                        DataTypes.ARRAY(DataTypes.STRING().notNull()).notNull()),
                field(
                        "col_nullable_array",
                        dbType("Array(Nullable(String))").notNull(),
                        DataTypes.ARRAY(DataTypes.STRING()).notNull()),
                field(
                        "col_map",
                        dbType("Map(String,String)").notNull(),
                        DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.STRING().notNull())
                                .notNull()),
                field(
                        "col_tuple",
                        dbType("Tuple(Int64, String)").notNull(),
                        DataTypes.ROW(DataTypes.BIGINT().notNull(), DataTypes.STRING().notNull())
                                .notNull()),
                field(
                        "col_complex",
                        dbType("Tuple(Array(Int64), Map(String,String))").notNull(),
                        DataTypes.ROW(
                                        DataTypes.ARRAY(DataTypes.BIGINT().notNull()).notNull(),
                                        DataTypes.MAP(
                                                        DataTypes.STRING().notNull(),
                                                        DataTypes.STRING().notNull())
                                                .notNull())
                                .notNull())
                //                field("col_nested", dbType("Nested"), DataTypes.ARRAY()),
                );
    }

    private static TableRow createGroupedTable(String tableName) {
        return tableRow(
                tableName,
                pkField("pid", dbType("Int64").notNull(), DataTypes.BIGINT().notNull()),
                field("col_bigint", dbType("Int64"), DataTypes.BIGINT()));
    }

    @BeforeEach
    void setup() {
        try (Connection conn = getMetadata().getConnection();
                Statement st = conn.createStatement()) {
            TABLE_ALL_TYPES.insertIntoTableValues(conn, TABLE_ALL_TYPES_ROWS);

            st.execute(String.format("CREATE DATABASE \"%s\" ENGINE=Atomic", TEST_DB2));
            st.execute(String.format("USE \"%s\"", TEST_DB2));
            st.execute(TABLE_PK2.getCreateQuery());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        String url = getMetadata().getJdbcUrl();
        url = url.lastIndexOf("?") > 0 ? url.substring(0, url.lastIndexOf("?")) : url;
        catalog =
                new ClickHouseCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        TEST_CATALOG_NAME,
                        TEST_DB,
                        getMetadata().getUsername(),
                        getMetadata().getPassword(),
                        url.substring(0, url.lastIndexOf("/")));

        this.tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        // Use ClickHouse catalog.
        tEnv.registerCatalog(TEST_CATALOG_NAME, catalog);
        tEnv.useCatalog(TEST_CATALOG_NAME);
    }

    @AfterEach
    void afterEach() {
        try (Connection conn = getMetadata().getConnection();
                Statement st = conn.createStatement()) {
            st.execute(String.format("DROP DATABASE IF EXISTS `%s`", TEST_DB2));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testGetDb_DatabaseNotExistException() {
        String databaseNotExist = "nonexistent";
        assertThatThrownBy(() -> catalog.getDatabase(databaseNotExist))
                .satisfies(
                        anyCauseMatches(
                                DatabaseNotExistException.class,
                                String.format(
                                        "Database %s does not exist in Catalog",
                                        databaseNotExist)));
    }

    @Test
    void testListDatabases() {
        List<String> actual = catalog.listDatabases();
        assertThat(actual).containsExactly("default", TEST_DB, TEST_DB2);
    }

    @Test
    void testDbExists() {
        String databaseNotExist = "nonexistent";
        assertThat(catalog.databaseExists(databaseNotExist)).isFalse();
        assertThat(catalog.databaseExists(TEST_DB)).isTrue();
    }

    // ------ tables ------

    @Test
    void testListTables() throws DatabaseNotExistException {
        List<String> actual = catalog.listTables(TEST_DB);
        assertThat(actual)
                .isEqualTo(
                        getManagedTables().stream()
                                .map(TableManaged::getTableName)
                                .collect(Collectors.toList()));
    }

    @Test
    void testListTables_DatabaseNotExistException() {
        String anyDatabase = "anyDatabase";
        assertThatThrownBy(() -> catalog.listTables(anyDatabase))
                .satisfies(
                        anyCauseMatches(
                                DatabaseNotExistException.class,
                                String.format(
                                        "Database %s does not exist in Catalog", anyDatabase)));
    }

    @Test
    void testTableExists() {
        String tableNotExist = "nonexist";
        assertThat(catalog.tableExists(new ObjectPath(TEST_DB, tableNotExist))).isFalse();
        assertThat(catalog.tableExists(new ObjectPath(TEST_DB, TABLE_ALL_TYPES.getTableName())))
                .isTrue();
    }

    @Test
    void testGetTables_TableNotExistException() {
        String anyTableNotExist = "anyTable";
        assertThatThrownBy(() -> catalog.getTable(new ObjectPath(TEST_DB, anyTableNotExist)))
                .satisfies(
                        anyCauseMatches(
                                TableNotExistException.class,
                                String.format(
                                        "Table (or view) %s.%s does not exist in Catalog",
                                        TEST_DB, anyTableNotExist)));
    }

    @Test
    void testGetTables_TableNotExistException_NoDb() {
        String databaseNotExist = "nonexistdb";
        String tableNotExist = "anyTable";
        assertThatThrownBy(() -> catalog.getTable(new ObjectPath(databaseNotExist, tableNotExist)))
                .satisfies(
                        anyCauseMatches(
                                TableNotExistException.class,
                                String.format(
                                        "Table (or view) %s.%s does not exist in Catalog",
                                        databaseNotExist, tableNotExist)));
    }

    @Test
    void testGetTable() throws TableNotExistException {
        CatalogBaseTable table =
                catalog.getTable(new ObjectPath(TEST_DB, TABLE_ALL_TYPES.getTableName()));

        assertThat(table.getUnresolvedSchema()).isEqualTo(TABLE_ALL_TYPES.getTableSchema());
    }

    @Test
    void testGetTablePrimaryKey() throws TableNotExistException {
        // test the PK of test.t_user
        Schema tableSchemaTestPK1 = TABLE_PK.getTableSchema();
        CatalogBaseTable tablePK1 =
                catalog.getTable(new ObjectPath(TEST_DB, TABLE_PK.getTableName()));
        assertThat(tableSchemaTestPK1.getPrimaryKey())
                .isEqualTo(tablePK1.getUnresolvedSchema().getPrimaryKey());

        // test the PK of TEST_DB2.t_user
        Schema tableSchemaTestPK2 = TABLE_PK2.getTableSchema();
        CatalogBaseTable tablePK2 =
                catalog.getTable(new ObjectPath(TEST_DB2, TABLE_PK2.getTableName()));
        assertThat(tableSchemaTestPK2.getPrimaryKey())
                .isEqualTo(tablePK2.getUnresolvedSchema().getPrimaryKey());
    }

    // ------ test select query. ------

    @Test
    void testSelectField() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select pid from %s",
                                                TABLE_ALL_TYPES.getTableName()))
                                .execute()
                                .collect());
        assertThat(results)
                .isEqualTo(
                        Arrays.asList(
                                Row.ofKind(RowKind.INSERT, 1L), Row.ofKind(RowKind.INSERT, 2L)));
    }

    @Test
    void testWithoutCatalogDB() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from %s", TABLE_ALL_TYPES.getTableName()))
                                .execute()
                                .collect());

        assertThat(results).isEqualTo(TABLE_ALL_TYPES_ROWS);
    }

    @Test
    void testWithoutCatalog() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from `%s`.`%s`",
                                                TEST_DB, TABLE_ALL_TYPES.getTableName()))
                                .execute()
                                .collect());
        assertThat(results).isEqualTo(TABLE_ALL_TYPES_ROWS);
    }

    @Test
    void testFullPath() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from %s.%s.`%s`",
                                                TEST_CATALOG_NAME,
                                                catalog.getDefaultDatabase(),
                                                TABLE_ALL_TYPES.getTableName()))
                                .execute()
                                .collect());
        assertThat(results).isEqualTo(TABLE_ALL_TYPES_ROWS);
    }

    @Test
    void testSelectToInsert() throws Exception {

        String sql =
                String.format(
                        "insert into `%s` select * from `%s`",
                        TABLE_ALL_TYPES_SINK.getTableName(), TABLE_ALL_TYPES.getTableName());
        tEnv.executeSql(sql).await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from %s",
                                                TABLE_ALL_TYPES_SINK.getTableName()))
                                .execute()
                                .collect());
        assertThat(results).isEqualTo(TABLE_ALL_TYPES_ROWS);
    }

    @Test
    void testGroupByInsert() throws Exception {
        // Changes primary key for the next record.
        tEnv.executeSql(
                        String.format(
                                "insert into `%s` select max(`pid`) `pid`, `col_bigint` from `%s` "
                                        + "group by `col_bigint` ",
                                TABLE_GROUPED_BY_SINK.getTableName(),
                                TABLE_ALL_TYPES.getTableName()))
                .await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from `%s`",
                                                TABLE_GROUPED_BY_SINK.getTableName()))
                                .execute()
                                .collect());
        assertThat(results)
                .isEqualTo(Collections.singletonList(Row.ofKind(RowKind.INSERT, 2L, -1L)));
    }
}
