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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalogTypeMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.clickhouse.utils.JdbcTypeUtil.clickhouseTypeToSqlType;
import static org.apache.flink.connector.jdbc.clickhouse.utils.JdbcTypeUtil.primitiveSqlTypeToDataTypes;

/** ClickHouseTypeMapper util class. */
@Internal
public class ClickHouseTypeMapper implements JdbcCatalogTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseTypeMapper.class);

    private final String databaseVersion;
    private final String driverVersion;

    public ClickHouseTypeMapper(String databaseVersion, String driverVersion) {
        this.databaseVersion = databaseVersion;
        this.driverVersion = driverVersion;
    }

    @Override
    public DataType mapping(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String typeName = metadata.getColumnTypeName(colIndex);
        int jdbcType = metadata.getColumnType(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        try {
            return sqlTypeToDataTypes(typeName, jdbcType, precision, scale);
        } catch (Exception e) {
            LOG.error(
                    "Failed to convert ClickHouse type {} into Flink data type {} for table {}.",
                    typeName,
                    jdbcType,
                    tablePath.getFullName(),
                    e);
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported ClickHouse type '%s' (sqlType: %s, precision: %s, scale: %s), in table '%s'. "
                                    + "driver version %s, database version %s. %s",
                            typeName,
                            jdbcType,
                            precision,
                            scale,
                            tablePath.getFullName(),
                            driverVersion,
                            databaseVersion,
                            e));
        }
    }

    public static DataType sqlTypeToDataTypes(
            String typeName, int sqlType, int precision, int scale) {
        boolean isNullable = false;
        if (typeName.toUpperCase().matches("NULLABLE\\(.*\\)")) {
            isNullable = true;
            typeName = parseNestedType(typeName).get(0);
        }
        switch (sqlType) {
            case Types.ARRAY:
                if (isNullable) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Nullable Array type is not supported in ClickHouse. Type got '%s'",
                                    typeName));
                }
                return DataTypes.ARRAY(extractEmbedType(typeName).get(0)).notNull();
            case Types.STRUCT:
                if (isNullable) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Nullable Struct type is not supported in ClickHouse. Type got '%s'",
                                    typeName));
                }
                if (typeName.toUpperCase().startsWith("MAP")) {
                    List<DataType> types = extractEmbedType(typeName);
                    if (types.size() != 2) {
                        throw new UnsupportedOperationException(
                                String.format(
                                        "Map type should have 2 types, but got %s. Type got '%s'",
                                        types.size(), typeName));
                    }
                    return DataTypes.MAP(types.get(0), types.get(1)).notNull();
                } else if (typeName.toUpperCase().startsWith("TUPLE")) {
                    return DataTypes.ROW(
                                    extractEmbedType(typeName).stream().toArray(DataType[]::new))
                            .notNull();
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Type not supported by ClickHouse JDBC connector. Type got '%s'",
                                    typeName));
                }
            default:
                return primitiveSqlTypeToDataTypes(sqlType, isNullable, precision, scale);
        }
    }

    private static List<DataType> extractEmbedType(String columnType) {
        return parseNestedType(columnType).stream()
                .map(
                        x -> {
                            String typeName = x.trim();
                            // probably with name? like 'int Int32'
                            if (typeName.matches("\\w+\\s+.*")) {
                                typeName = typeName.substring(typeName.indexOf(" ") + 1);
                            }
                            String actualTypeName = typeName;
                            if (typeName.toUpperCase().matches("NULLABLE\\(.*\\)")) {
                                actualTypeName = parseNestedType(typeName).get(0);
                            }
                            int startEmbed = actualTypeName.indexOf("(");
                            int endPrecision = actualTypeName.indexOf(",");
                            int endEmbed = actualTypeName.lastIndexOf(")");
                            int precision = 0;
                            int scale = 0;
                            if (startEmbed > 0) {
                                try {

                                    precision =
                                            Integer.parseInt(
                                                    actualTypeName.substring(
                                                            startEmbed + 1, endPrecision));
                                    scale =
                                            Integer.parseInt(
                                                    actualTypeName.substring(
                                                            endPrecision + 1, endEmbed));
                                    return sqlTypeToDataTypes(
                                            typeName,
                                            clickhouseTypeToSqlType(
                                                    actualTypeName.substring(0, startEmbed)),
                                            precision,
                                            scale);
                                } catch (IndexOutOfBoundsException | NumberFormatException e) {
                                    return sqlTypeToDataTypes(
                                            typeName,
                                            clickhouseTypeToSqlType(
                                                    actualTypeName.substring(0, startEmbed)),
                                            precision,
                                            scale);
                                }
                            }
                            return sqlTypeToDataTypes(
                                    typeName, clickhouseTypeToSqlType(actualTypeName), 0, 0);
                        })
                .collect(Collectors.toList());
    }

    private static List<String> parseNestedType(String nestedTypeString) {
        return parseNestedType(nestedTypeString, 1);
    }

    private static List<String> parseNestedType(String nestedTypeString, int targetDepth) {
        List<String> parts = new ArrayList<>();
        int depthStart = 0;
        int depth = 0;
        for (int i = 0; i < nestedTypeString.length(); i++) {
            if (nestedTypeString.charAt(i) == '(') {
                depth++;
                if (depth == targetDepth) {
                    depthStart = i + 1;
                }
            } else if (nestedTypeString.charAt(i) == ')') {
                if (depth == targetDepth) {
                    parts.add(nestedTypeString.substring(depthStart, i).trim());
                    depthStart = i + 2;
                }
                depth--;
            } else if (nestedTypeString.charAt(i) == ',') {
                if (depth == targetDepth) {
                    parts.add(nestedTypeString.substring(depthStart, i).trim());
                    depthStart = i + 1;
                }
            }
            if (i == nestedTypeString.length() - 1) {
                if (depth != 0) {
                    throw new IllegalArgumentException(
                            "Possible wrong ClickHouse nested type: " + nestedTypeString);
                }
                if (depthStart < nestedTypeString.length()) {
                    parts.add(nestedTypeString.substring(depthStart).trim());
                }
            }
        }
        return parts;
    }
}
