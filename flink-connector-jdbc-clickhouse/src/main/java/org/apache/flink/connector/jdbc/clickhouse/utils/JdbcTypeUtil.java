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

package org.apache.flink.connector.jdbc.clickhouse.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;

import com.clickhouse.data.ClickHouseDataType;

import java.sql.Types;

/** Utils for ClickHouse jdbc type. */
@Internal
public class JdbcTypeUtil {
    private static final int RAW_TIMESTAMP_LENGTH = 19;

    public static int clickhouseTypeToSqlType(String clickhouseType)
            throws IllegalArgumentException {
        int sqlType = Types.OTHER;
        switch (ClickHouseDataType.of(clickhouseType)) {
            case Bool:
                sqlType = Types.BOOLEAN;
                break;
            case Int8:
                sqlType = Types.TINYINT;
                break;
            case UInt8:
            case Int16:
                sqlType = Types.SMALLINT;
                break;
            case UInt16:
            case Int32:
                sqlType = Types.INTEGER;
                break;
            case UInt32:
            case IntervalYear:
            case IntervalQuarter:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalMinute:
            case IntervalSecond:
            case IntervalMicrosecond:
            case IntervalMillisecond:
            case IntervalNanosecond:
            case Int64:
                sqlType = Types.BIGINT;
                break;
            case UInt64:
            case Int128:
            case UInt128:
            case Int256:
            case UInt256:
                sqlType = Types.NUMERIC;
                break;
            case Float32:
                sqlType = Types.FLOAT;
                break;
            case Float64:
                sqlType = Types.DOUBLE;
                break;
            case Decimal:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal256:
                sqlType = Types.DECIMAL;
                break;
            case Date:
            case Date32:
                sqlType = Types.DATE;
                break;
            case DateTime:
            case DateTime32:
            case DateTime64:
                sqlType = Types.TIMESTAMP;
                break;
            case Enum8:
            case Enum16:
            case IPv4:
            case IPv6:
            case FixedString:
            case JSON:
            case Object:
            case String:
            case UUID:
                sqlType = Types.VARCHAR;
                break;
            case Point:
            case Ring:
            case Polygon:
            case MultiPolygon:
            case Array:
                sqlType = Types.ARRAY;
                break;
            case Map: // Map<?,?>
            case Nested: // Object[][]
            case Tuple: // List<?>
                sqlType = Types.STRUCT;
                break;
            case Nothing:
                sqlType = Types.NULL;
                break;
            default:
                throw new IllegalArgumentException("Unsupported ClickHouse type: " + sqlType);
        }
        return sqlType;
    }

    public static DataType primitiveSqlTypeToDataTypes(
            int sqlType, boolean nullable, int precision, int scale) {
        DataType dataType = primitiveSqlTypeToDataTypes(sqlType, precision, scale);
        if (nullable) {
            return dataType.nullable();
        } else {
            return dataType.notNull();
        }
    }

    public static DataType primitiveSqlTypeToDataTypes(int sqlType, int precision, int scale) {
        switch (sqlType) {
            case Types.BOOLEAN:
                return DataTypes.BOOLEAN();
            case Types.TINYINT:
                return DataTypes.TINYINT();
            case Types.SMALLINT:
                return DataTypes.SMALLINT();
            case Types.INTEGER:
                return DataTypes.INT();
            case Types.BIGINT:
                return DataTypes.BIGINT();
            case Types.NUMERIC:
            case Types.DECIMAL:
                return getDecimalType(precision, scale);
            case Types.REAL:
            case Types.FLOAT:
                return DataTypes.FLOAT();
            case Types.DOUBLE:
                return DataTypes.DOUBLE();
            case Types.DATE:
                return DataTypes.DATE();
            case Types.TIMESTAMP_WITH_TIMEZONE:
            case Types.TIMESTAMP:
                return isExplicitPrecision(precision, RAW_TIMESTAMP_LENGTH)
                        ? DataTypes.TIMESTAMP(precision - RAW_TIMESTAMP_LENGTH - 1)
                        : DataTypes.TIMESTAMP(0);
            case Types.CHAR:
            case Types.NCHAR:
                return DataTypes.CHAR(precision);
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
                return precision > 0 ? DataTypes.VARCHAR(precision) : DataTypes.STRING();
            case Types.CLOB:
                return DataTypes.STRING();
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return DataTypes.BYTES();
            default:
                throw new IllegalArgumentException("Unsupported ClickHouse sqlType: " + sqlType);
        }
    }

    private static DataType getDecimalType(int precision, int scale) {
        if (precision >= DecimalType.MAX_PRECISION || precision == 0) {
            return DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 0);
        }
        return DataTypes.DECIMAL(precision, scale);
    }

    private static boolean isExplicitPrecision(int precision, int defaultPrecision) {

        return precision > defaultPrecision && precision - defaultPrecision - 1 <= 9;
    }
}
