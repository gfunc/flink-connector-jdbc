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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.core.database.catalog.AbstractJdbcCatalog;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalogTypeMapper;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TemporaryClassLoaderContext;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.connector.jdbc.JdbcConnectionOptions.getBriefAuthProperties;

/** Catalog for ClickHouse. */
@Internal
public class ClickHouseCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseCatalog.class);

    private final JdbcCatalogTypeMapper dialectTypeMapper;

    private static final Set<String> builtinDatabases =
            new HashSet<String>() {
                {
                    add("information_schema");
                    add("INFORMATION_SCHEMA");
                    add("system");
                }
            };

    @VisibleForTesting
    public ClickHouseCatalog(
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

    public ClickHouseCatalog(
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
        this.dialectTypeMapper = new ClickHouseTypeMapper(databaseVersion, driverVersion);
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
                        String.format("Failed in getting ClickHouse version by %s.", defaultUrl),
                        e);
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
                        String.format(
                                "Failed in getting ClickHouse driver version by %s.", defaultUrl),
                        e);
            }
        }
    }

    /** Converts ClickHouse type to Flink {@link DataType}. */
    @Override
    protected DataType fromJDBCType(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        return dialectTypeMapper.mapping(tablePath, metadata, colIndex);
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

    /** */
    @Override
    protected Optional<UniqueConstraint> getPrimaryKey(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {
        // SQL Copied from ClickHouse JDBC driver v2 with
        // Order by KEY_SEQ to ensure the order of primary key fields
        //
        try {
            String sql =
                    "SELECT NULL AS TABLE_CAT, "
                            + "system.tables.database AS TABLE_SCHEM, "
                            + "system.tables.name AS TABLE_NAME, "
                            + "trim(c.1) AS COLUMN_NAME, "
                            + "c.2 AS KEY_SEQ, "
                            + "'PRIMARY' AS PK_NAME "
                            + "FROM system.tables "
                            + "ARRAY JOIN arrayZip(splitByChar(',', primary_key), arrayEnumerate(splitByChar(',', primary_key))) as c "
                            + "WHERE system.tables.primary_key <> '' "
                            + "AND system.tables.database ILIKE '"
                            + (schema == null ? "%" : schema)
                            + "' "
                            + "AND system.tables.name ILIKE '"
                            + (table == null ? "%" : table)
                            + "' "
                            + "ORDER BY KEY_SEQ";
            ResultSet rs = metaData.getConnection().createStatement().executeQuery(sql);
            List<String> pkFields = new ArrayList<>();
            String pkName = null;
            while (rs.next()) {
                pkFields.add(rs.getString("COLUMN_NAME"));
                pkName = rs.getString("PK_NAME");
            }
            if (!pkFields.isEmpty()) {
                pkName = pkName == null ? "pk_" + String.join("_", pkFields) : pkName;
                return Optional.of(UniqueConstraint.primaryKey(pkName, pkFields));
            }

        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting primary key of table %s", table), e);
        }
        return Optional.empty();
    }
}
