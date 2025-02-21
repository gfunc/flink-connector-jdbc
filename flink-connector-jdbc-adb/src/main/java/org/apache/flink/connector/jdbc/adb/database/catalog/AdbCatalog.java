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

package org.apache.flink.connector.jdbc.adb.database.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.mysql.database.catalog.MySqlCatalog;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.JdbcCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.Preconditions;

import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/** Catalog for ADB. */
@Internal
public class AdbCatalog extends MySqlCatalog {
    private static final String hiddenPrimaryKey = "__adb_auto_id__";
    private static final Set<String> builtinDatabases =
            new HashSet<String>() {
                {
                    add("information_schema");
                    add("INFORMATION_SCHEMA");
                    add("mysql");
                    add("performance_schema");
                    add("sys");
                    add("MYSQL");
                }
            };

    @VisibleForTesting
    public AdbCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        super(userClassLoader, catalogName, defaultDatabase, username, pwd, baseUrl);
    }

    public AdbCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String baseUrl,
            Properties connectionProperties) {
        super(userClassLoader, catalogName, defaultDatabase, baseUrl, connectionProperties);
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        Preconditions.checkState(
                StringUtils.isNotBlank(databaseName), "Database name must not be blank.");
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        return extractColumnValuesBySQL(
                getDatabaseUrl(databaseName),
                "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA = ?",
                1,
                null,
                databaseName);
    }

    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        return extractColumnValuesBySQL(
                getDatabaseUrl(databaseName),
                "SELECT TABLE_NAME FROM information_schema.`VIEWS` WHERE TABLE_SCHEMA = ?",
                1,
                null,
                databaseName);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return extractColumnValuesBySQL(
                defaultUrl,
                "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA`;",
                1,
                dbName -> !builtinDatabases.contains(dbName));
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {

        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        String databaseName = tablePath.getDatabaseName();

        try (Connection conn =
                DriverManager.getConnection(getDatabaseUrl(databaseName), connectionProperties)) {
            DatabaseMetaData metaData = conn.getMetaData();
            Optional<UniqueConstraint> primaryKey =
                    getPrimaryKey(
                            metaData,
                            databaseName,
                            getSchemaName(tablePath),
                            getTableName(tablePath));

            Statement st = conn.createStatement();
            // get table comment
            String tableComment = "";
            try (ResultSet rs =
                    st.executeQuery(
                            String.format(
                                    "SELECT table_comment\n"
                                            + "FROM INFORMATION_SCHEMA.tables\n"
                                            + "WHERE table_schema = '%s'\n"
                                            + "  AND table_name = '%s';",
                                    getSchemaName(tablePath), getTableName(tablePath)))) {
                while (rs.next()) {
                    tableComment = rs.getString(1);
                }
            }
            // get column extra info
            Map<String, Optional<String>> columnComments = new HashMap<>();
            Map<String, Optional<String>> columnDefaults = new HashMap<>();
            Map<String, Optional<String>> columnExtras = new HashMap<>();
            Map<String, Optional<Integer>> columnVarcharLengthens = new HashMap<>();
            try (ResultSet rs =
                    st.executeQuery(
                            String.format(
                                    "SELECT column_name, column_comment, column_default, extra, character_maximum_length \n"
                                            + "FROM INFORMATION_SCHEMA.columns\n"
                                            + "WHERE table_schema = '%s'\n"
                                            + "  AND table_name = '%s';",
                                    getSchemaName(tablePath), getTableName(tablePath)))) {
                while (rs.next()) {
                    columnComments.put(
                            rs.getString(1),
                            Optional.ofNullable(
                                    rs.getString(2) != null && rs.getString(2).isEmpty()
                                            ? null
                                            : rs.getString(2)));
                    String columnDefault = rs.getString(3);
                    if (columnDefault != null) {
                        if (columnDefault.matches(
                                "^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}.*$")) {
                            columnDefault = "CURRENT_TIMESTAMP";
                        }
                    }
                    columnDefaults.put(rs.getString(1), Optional.ofNullable(columnDefault));

                    columnExtras.put(
                            rs.getString(1),
                            Optional.ofNullable(
                                    rs.getString(4) != null && rs.getString(4).isEmpty()
                                            ? null
                                            : rs.getString(4)));
                    columnVarcharLengthens.put(
                            rs.getString(1),
                            Optional.ofNullable(rs.getInt(5) == 0 ? null : rs.getInt(5)));
                }
            }
            // get not null columns, metadata is_nullable is not implemented by ADB
            Set<String> notNulls = new HashSet<>();
            try (ResultSet rs =
                    st.executeQuery(
                            String.format(
                                    "SELECT column_name\n"
                                            + "FROM INFORMATION_SCHEMA.columns\n"
                                            + "WHERE table_schema = '%s'\n"
                                            + "  AND table_name = '%s'\n"
                                            + "  AND is_nullable = 'NO'\n"
                                            + "order by ordinal_position",
                                    getSchemaName(tablePath), getTableName(tablePath)))) {
                while (rs.next()) {
                    notNulls.add(rs.getString(1));
                }
            }

            String stmt = String.format("SELECT * FROM `%s`;", getSchemaTableName(tablePath));

            Set<String> keys = new TreeSet<>();
            primaryKey.ifPresent(t -> t.getColumns().forEach(keys::add));

            // if hidden primary key
            if (primaryKey.isPresent()
                    && primaryKey.get().getColumns().get(0).equals(hiddenPrimaryKey)) {
                stmt =
                        String.format(
                                "SELECT %s,* FROM `%s`;",
                                hiddenPrimaryKey, getSchemaTableName(tablePath));
                columnComments.put(hiddenPrimaryKey, Optional.empty());
                columnDefaults.put(hiddenPrimaryKey, Optional.empty());
                columnExtras.put(hiddenPrimaryKey, Optional.of("auto_increment"));
                columnVarcharLengthens.put(hiddenPrimaryKey, Optional.empty());
            }

            PreparedStatement ps = conn.prepareStatement(stmt);
            ResultSetMetaData resultSetMetaData = ps.getMetaData();

            Schema.Builder schemaBuilder = Schema.newBuilder();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                String columnName = resultSetMetaData.getColumnName(i);
                if (columnName == null) {
                    continue;
                }
                DataType dataType = fromJDBCType(tablePath, resultSetMetaData, i);

                if (primaryKey.isPresent()
                        && !primaryKey.get().getColumns().contains(columnName)
                        && dataType.getLogicalType().getTypeRoot() == LogicalTypeRoot.VARCHAR) {
                    if (columnVarcharLengthens.get(columnName).isPresent()) {
                        dataType = DataTypes.VARCHAR(columnVarcharLengthens.get(columnName).get());
                    } else if (!columnDefaults.get(columnName).isPresent()) {
                        dataType = DataTypes.STRING();
                    }
                }
                // skip nullable primary keys in adb
                if (keys.contains(columnName)) {
                    dataType = dataType.notNull();
                }
                if (resultSetMetaData.isNullable(i) == ResultSetMetaData.columnNoNulls) {
                    dataType = dataType.notNull();
                }
                if (notNulls.contains(columnName)) {
                    dataType = dataType.notNull();
                }
                schemaBuilder.column(columnName, dataType);
                columnComments.get(columnName).ifPresent(schemaBuilder::withComment);
            }

            primaryKey.ifPresent(
                    pk -> schemaBuilder.primaryKeyNamed(pk.getName(), pk.getColumns()));
            Schema tableSchema = schemaBuilder.build();

            return new JdbcCatalogTable(
                    "ADB",
                    tableSchema,
                    tableComment,
                    Lists.newArrayList(),
                    getOptions(tablePath),
                    columnDefaults,
                    columnExtras);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }
}
