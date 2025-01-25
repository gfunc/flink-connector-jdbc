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

import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test class for {@link ClickHouseTypeMapper}. */
public class ClickHouseTypeMapperTest {
    @Test
    void sqlTypeToDataTypes_shouldReturnArrayType() {
        assertEquals(
                TIMESTAMP(3).notNull(),
                ClickHouseTypeMapper.sqlTypeToDataTypes(
                        "DateTime64(3,'Asia/Shanghai')", Types.TIMESTAMP, 23, 0));

        assertEquals(
                ARRAY(INT().notNull()).notNull(),
                ClickHouseTypeMapper.sqlTypeToDataTypes("Array(Int32)", Types.ARRAY, 0, 0));
        assertEquals(
                ARRAY(DECIMAL(12, 2).notNull()).notNull(),
                ClickHouseTypeMapper.sqlTypeToDataTypes("Array(Decimal(12,2))", Types.ARRAY, 0, 0));
        assertEquals(
                ROW(INT().notNull(), STRING().notNull()).notNull(),
                ClickHouseTypeMapper.sqlTypeToDataTypes(
                        "Tuple(Int32, String)", Types.STRUCT, 0, 0));
        assertEquals(
                ROW(INT().notNull(), DECIMAL(14, 4).notNull()).notNull(),
                ClickHouseTypeMapper.sqlTypeToDataTypes(
                        "Tuple(Int32, Decimal(14,4))", Types.STRUCT, 0, 0));

        assertEquals(
                ROW(ARRAY(INT().notNull()).notNull(), DECIMAL(14, 4).notNull()).notNull(),
                ClickHouseTypeMapper.sqlTypeToDataTypes(
                        "Tuple(Array(Int32), Decimal(14,4))", Types.STRUCT, 0, 0));

        assertEquals(
                ROW(ARRAY(INT().notNull()).notNull(), DECIMAL(14, 4).notNull()).notNull(),
                ClickHouseTypeMapper.sqlTypeToDataTypes(
                        "Tuple(a Array(Int32), b Decimal(14,4))", Types.STRUCT, 0, 0));

        assertEquals(
                ARRAY(ROW(ARRAY(INT().notNull()).notNull(), DECIMAL(14, 4).notNull()).notNull())
                        .notNull(),
                ClickHouseTypeMapper.sqlTypeToDataTypes(
                        "Array(Tuple(a Array(Int32), b Decimal(14,4)))", Types.ARRAY, 0, 0));
        assertEquals(
                DECIMAL(10, 2).notNull(),
                ClickHouseTypeMapper.sqlTypeToDataTypes("Decimal", Types.DECIMAL, 10, 2));
        // wrong types
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ClickHouseTypeMapper.sqlTypeToDataTypes(
                                "Array(Decimal(a,b)", Types.ARRAY, 0, 0));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ClickHouseTypeMapper.sqlTypeToDataTypes(
                                "Array(InvalidType)", Types.ARRAY, 0, 0));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ClickHouseTypeMapper.sqlTypeToDataTypes(
                                "Tuple(InvalidType)", Types.STRUCT, 0, 0));
        // nullable types
        assertEquals(
                INT(),
                ClickHouseTypeMapper.sqlTypeToDataTypes("Nullable(Int32)", Types.INTEGER, 0, 0));

        assertEquals(
                ARRAY(ROW(ARRAY(INT().nullable()).notNull(), DECIMAL(14, 4).nullable()).notNull())
                        .notNull(),
                ClickHouseTypeMapper.sqlTypeToDataTypes(
                        "Array(Tuple(a Array(Nullable(Int32)), b Nullable(Decimal(14,4))))",
                        Types.ARRAY,
                        0,
                        0));
    }
}
