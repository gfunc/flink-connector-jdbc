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

package org.apache.flink.connector.jdbc.mysql.database.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.core.database.catalog.AbstractJdbcCatalog;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalogTypeMapper;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Constraint;
import org.apache.flink.table.catalog.JdbcCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TemporaryClassLoaderContext;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.JdbcConnectionOptions.getBriefAuthProperties;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE;

/** Catalog for MySQL. */
@Internal
public class MySqlCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlCatalog.class);

    private final JdbcCatalogTypeMapper dialectTypeMapper;

    private static final Set<String> builtinDatabases =
            new HashSet<String>() {
                {
                    add("information_schema");
                    add("mysql");
                    add("performance_schema");
                    add("sys");
                }
            };

    @VisibleForTesting
    public MySqlCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        this(
                userClassLoader,
                catalogName,
                defaultDatabase,
                baseUrl,
                getBriefAuthProperties(username, pwd));
    }

    public MySqlCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String baseUrl,
            Properties connectionProperties) {
        super(userClassLoader, catalogName, defaultDatabase, baseUrl, connectionProperties);

        String driverVersion =
                Preconditions.checkNotNull(getDriverVersion(), "Driver version must not be null.");
        String databaseVersion =
                Preconditions.checkNotNull(
                        getDatabaseVersion(), "Database version must not be null.");
        LOG.info("Driver version: {}, database version: {}", driverVersion, databaseVersion);
        this.dialectTypeMapper = new MySqlTypeMapper(databaseVersion, driverVersion);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return extractColumnValuesBySQL(
                defaultUrl,
                "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA`;",
                1,
                dbName -> !builtinDatabases.contains(dbName));
    }

    // ------ tables ------

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
                "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_SCHEMA = ?",
                1,
                null,
                databaseName);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return !extractColumnValuesBySQL(
                        baseUrl,
                        "SELECT TABLE_NAME FROM information_schema.`TABLES` "
                                + "WHERE TABLE_SCHEMA=? and TABLE_NAME=?",
                        1,
                        null,
                        tablePath.getDatabaseName(),
                        tablePath.getObjectName())
                .isEmpty();
    }

    private String getDatabaseVersion() {
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(userClassLoader)) {
            try (Connection conn = DriverManager.getConnection(defaultUrl, connectionProperties)) {
                return conn.getMetaData().getDatabaseProductVersion();
            } catch (Exception e) {
                throw new CatalogException(
                        String.format("Failed in getting MySQL version by %s.", defaultUrl), e);
            }
        }
    }

    private String getDriverVersion() {
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(userClassLoader)) {
            try (Connection conn = DriverManager.getConnection(defaultUrl, connectionProperties)) {
                String driverVersion = conn.getMetaData().getDriverVersion();
                Pattern regexp = Pattern.compile("\\d+?\\.\\d+?\\.\\d+");
                Matcher matcher = regexp.matcher(driverVersion);
                return matcher.find() ? matcher.group(0) : null;
            } catch (Exception e) {
                throw new CatalogException(
                        String.format("Failed in getting MySQL driver version by %s.", defaultUrl),
                        e);
            }
        }
    }

    /** Converts MySQL type to Flink {@link DataType}. */
    @Override
    protected DataType fromJDBCType(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        return dialectTypeMapper.mapping(tablePath, metadata, colIndex);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        if (databaseExists(name)) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(getName(), name);
            }
            return;
        }
        try (Connection conn = DriverManager.getConnection(defaultUrl, connectionProperties)) {
            conn.createStatement().execute("CREATE SCHEMA " + name);
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed to create database %s", name), e);
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

        if (!databaseExists(getSchemaName(tablePath))) {
            throw new DatabaseNotExistException(getName(), getSchemaName(tablePath));
        }
        if (tableExists(tablePath)) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(getName(), tablePath);
            }
            return;
        }
        String tableComment = table.getComment();
        int varcharTotalLength = 0;
        if (table instanceof ResolvedCatalogTable) {
            ResolvedSchema schema = ((ResolvedCatalogTable) table).getResolvedSchema();
            CatalogTable origin = ((ResolvedCatalogTable) table).getOrigin();
            StringBuilder sql =
                    new StringBuilder(
                            String.format(
                                    "CREATE TABLE %s.%s (\n",
                                    EncodingUtils.escapeIdentifier(getSchemaName(tablePath)),
                                    EncodingUtils.escapeIdentifier(getTableName(tablePath))));
            for (int i = 0; i < schema.getColumnCount(); i++) {
                Column column = schema.getColumn(i).isPresent() ? schema.getColumn(i).get() : null;
                StringBuilder sb = new StringBuilder();
                String columnType = column.getDataType().toString();
                LogicalType dataType = column.getDataType().getLogicalType();
                switch (dataType.getTypeRoot()) {
                    case VARCHAR:
                        int length = ((VarCharType) dataType).getLength();
                        if (columnType.equalsIgnoreCase("STRING")){
                            columnType = "TEXT";
                        }else{
                            columnType = String.format("VARCHAR(%s)", length);
                        }
                        int lengthInByte = (length * 4);
                        if (schema.getPrimaryKey().isPresent()
                                && schema.getPrimaryKey()
                                        .get()
                                        .getColumns()
                                        .contains(column.getName())) {
                            varcharTotalLength += lengthInByte;
                            break;
                        }
                        // to avoid row size too large error
                        if (varcharTotalLength + lengthInByte > 50000) {
                            columnType = "TEXT";
                        } else {
                            varcharTotalLength += lengthInByte;
                        }
                        break;
                        //                    case CHAR:
                    case BINARY:
                    case VARBINARY:
                        columnType = "BLOB";
                        break;
                        //                    case BOOLEAN:
                        //                    case TINYINT:
                        //                    case SMALLINT:
                        //                    case INTEGER:
                        //                    case BIGINT:
                        //                    case FLOAT:
                        //                    case DOUBLE:
                        //                    case DATE:
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                        //                        Types.TIMESTAMP);
                    case TIMESTAMP_WITH_TIME_ZONE:
                        //                        Types.TIMESTAMP_WITH_TIMEZONE);
                    case TIME_WITHOUT_TIME_ZONE:
                        columnType = "DATETIME";
                        break;
                        //                    case DECIMAL:
                        //                    case ARRAY:
                        //                    default:
                }
                sb.append(EncodingUtils.escapeIdentifier(column.getName()));
                sb.append(" ");
                sb.append(columnType);
                if (origin instanceof JdbcCatalogTable) {
                    JdbcCatalogTable jdbcCatalogTable = (JdbcCatalogTable) origin;
                    String sourceType = jdbcCatalogTable.getSourceType();
                    if ("ADB".equalsIgnoreCase(sourceType)
                            || "MYSQL".equalsIgnoreCase(sourceType)) {
                        tableComment = jdbcCatalogTable.getComment();
                        jdbcCatalogTable
                                .getSourceDefault()
                                .get(column.getName())
                                .ifPresent(
                                        t -> {
                                            sb.append(" DEFAULT ");
                                            if (t.equalsIgnoreCase("CURRENT_TIMESTAMP")) {
                                                sb.append(t);
                                            } else {
                                                sb.append("'");
                                                sb.append(t);
                                                sb.append("'");
                                            }
                                        });
                        jdbcCatalogTable
                                .getSourceExtra()
                                .get(column.getName())
                                .ifPresent(t -> sb.append(" ").append(t.toUpperCase()));
                    }
                }
                column.getComment()
                        .ifPresent(
                                c -> {
                                    sb.append(" COMMENT '");
                                    sb.append(EncodingUtils.escapeSingleQuotes(c));
                                    sb.append("'");
                                });
                sql.append(sb);
                if (i != schema.getColumnCount() - 1) {
                    sql.append(", \n");
                }
            }
            schema.getPrimaryKey()
                    .ifPresent(
                            t -> {
                                sql.append(",\n");
                                if (t.getType() == Constraint.ConstraintType.UNIQUE_KEY) {
                                    sql.append("UNIQUE KEY ");
                                    sql.append(EncodingUtils.escapeIdentifier(t.getName()));
                                    sql.append(" ");
                                } else {
                                    sql.append("PRIMARY KEY ");
                                }
                                sql.append("(");
                                sql.append(
                                        t.getColumns().stream()
                                                .map(EncodingUtils::escapeIdentifier)
                                                .collect(Collectors.joining(", ")));
                                sql.append(")\n");
                            });

            sql.append(")");
            if (!tableComment.isEmpty()) {
                sql.append(" COMMENT = '");
                sql.append(EncodingUtils.escapeSingleQuotes(tableComment));
                sql.append("'");
            }
            LOG.debug(
                    "varchar total length for table {}: {}",
                    getTableName(tablePath),
                    varcharTotalLength);
            try (Connection conn = DriverManager.getConnection(defaultUrl, connectionProperties)) {
                conn.createStatement().execute(sql.toString());
            } catch (SQLException e) {
                LOG.debug("SQL {}", sql);
                throw new CatalogException(
                        String.format(
                                "Failed to create table %s.%s",
                                tablePath.getDatabaseName(), tablePath.getObjectName()),
                        e);
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    protected String getTableName(ObjectPath tablePath) {
        return tablePath.getObjectName();
    }

    @Override
    protected String getSchemaName(ObjectPath tablePath) {
        return tablePath.getDatabaseName();
    }

    @Override
    protected String getSchemaTableName(ObjectPath tablePath) {
        return tablePath.getObjectName();
    }
}
