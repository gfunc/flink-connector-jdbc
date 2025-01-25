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

package org.apache.flink.connector.jdbc.clickhouse.database.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialectConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;

/** JDBC dialect for ClickHouse. * */
@Internal
public class ClickHouseDialect extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    // Define MAX/MIN precision of TIMESTAMP type according to ClickHouse docs:
    // https://clickhouse.com/docs/en/sql-reference/data-types/datetime64
    private static final int MAX_TIMESTAMP_PRECISION = 9;
    private static final int MIN_TIMESTAMP_PRECISION = 0;

    // Define MAX/MIN precision of DECIMAL type according to ClickHouse docs:
    // https://clickhouse.com/docs/en/sql-reference/data-types/decimal
    private static final int MAX_DECIMAL_PRECISION = 76;
    private static final int MIN_DECIMAL_PRECISION = 1;

    @Override
    public JdbcDialectConverter getRowConverter(RowType rowType) {
        return new ClickHouseDialectConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "LIMIT " + limit;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.clickhouse.jdbc.ClickHouseDriver");
    }

    @Override
    public String dialectName() {
        return "ClickHouse";
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.empty();
    }

    /**
     * ClickHouse JDBC would convert update to an ALTER TABLE UPDATE mutation. This is expensive and
     * not recommended.
     */
    @Override
    public String getUpdateStatement(
            String tableName, String[] fieldNames, String[] conditionFields) {
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        String setClause =
                Arrays.stream(fieldNames)
                        .filter(field -> !conditionClause.contains(field))
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(", "));

        return "UPDATE "
                + quoteIdentifier(tableName)
                + " SET "
                + setClause
                + " WHERE "
                + conditionClause;
    }

    @Override
    public Optional<Range> decimalPrecisionRange() {
        return Optional.of(Range.of(MIN_DECIMAL_PRECISION, MAX_DECIMAL_PRECISION));
    }

    @Override
    public Optional<Range> timestampPrecisionRange() {
        return Optional.of(Range.of(MIN_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION));
    }

    @Override
    public Set<LogicalTypeRoot> supportedTypes() {
        // The data types used in ClickHouse are list at:
        // https://clickhouse.com/docs/en/sql-reference/data-types

        return EnumSet.of(
                LogicalTypeRoot.BOOLEAN,
                LogicalTypeRoot.TINYINT,
                LogicalTypeRoot.SMALLINT,
                LogicalTypeRoot.INTEGER,
                LogicalTypeRoot.BIGINT,
                LogicalTypeRoot.DOUBLE,
                LogicalTypeRoot.DECIMAL,
                LogicalTypeRoot.VARCHAR,
                LogicalTypeRoot.FLOAT,
                LogicalTypeRoot.CHAR,
                LogicalTypeRoot.VARBINARY,
                LogicalTypeRoot.DATE,
                LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
                LogicalTypeRoot.ARRAY,
                LogicalTypeRoot.MAP,
                LogicalTypeRoot.ROW);
    }
}
