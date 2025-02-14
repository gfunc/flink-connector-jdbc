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

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.testutils.tables.TableField;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import com.clickhouse.data.Tuple;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** TableRow for ClickHouse. */
public class ClickHouseTableRow extends TableRow {

    private final String pkFields;

    public ClickHouseTableRow(String name, TableField[] fields) {
        super(name, fields);
        this.pkFields =
                Arrays.stream(fields)
                        .filter(TableField::isPkField)
                        .map(TableField::getName)
                        .collect(Collectors.joining(", "));
    }

    @Override
    protected String getDeleteFromQuery() {
        return String.format("DELETE FROM %s WHERE 1=1", getTableName());
    }

    @Override
    public String getCreateQuery() {
        return String.format(
                "CREATE TABLE %s (%s) ENGINE = MergeTree() %s",
                getTableName(),
                Arrays.stream(getFields())
                        .map(TableField::asString)
                        .collect(Collectors.joining(", ")),
                pkFields.isEmpty()
                        ? "ORDER BY tuple()"
                        : String.format("ORDER BY (%s) PRIMARY KEY (%s)", pkFields, pkFields));
    }

    private final JdbcStatementBuilder<Row> statementBuilder =
            (ps, row) -> {
                DataTypes.Field[] fields = getTableDataFields();
                for (int i = 0; i < row.getArity(); i++) {
                    DataType type = fields[i].getDataType();
                    int dbType =
                            JdbcTypeUtil.logicalTypeToSqlType(type.getLogicalType().getTypeRoot());
                    if (row.getField(i) == null) {
                        ps.setNull(i + 1, dbType);
                    } else {
                        if (type.getConversionClass().equals(LocalTime.class)) {
                            Time time = Time.valueOf(row.<LocalTime>getFieldAs(i));
                            ps.setTime(i + 1, time);
                        } else if (type.getConversionClass().equals(LocalDate.class)) {
                            ps.setDate(i + 1, Date.valueOf(row.<LocalDate>getFieldAs(i)));
                        } else if (type.getConversionClass().equals(LocalDateTime.class)) {
                            ps.setTimestamp(
                                    i + 1, Timestamp.valueOf(row.<LocalDateTime>getFieldAs(i)));
                        } else if (type.getConversionClass().equals(Row.class)) {

                            Row tuple = row.getFieldAs(i);
                            List<Object> t = new ArrayList<>();
                            for (int j = 0; j < tuple.getArity(); j++) {
                                t.add(tuple.getField(j));
                            }

                            ps.setObject(i + 1, new Tuple(t.toArray()), java.sql.Types.STRUCT);
                            //                            ps.setObject(i + 1, t.toArray());
                        } else {
                            ps.setObject(i + 1, row.getField(i));
                        }
                    }
                }
            };

    public void insertIntoTableValues(Connection conn, List<Row> values) throws SQLException {
        executeStatement(conn, getInsertIntoQuery(), statementBuilder, values);
    }
}
