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
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialectConverter;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import com.clickhouse.data.value.UnsignedByte;
import com.clickhouse.data.value.UnsignedInteger;
import com.clickhouse.data.value.UnsignedShort;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * ClickHouse.
 */
@Internal
public class ClickHouseDialectConverter extends AbstractDialectConverter {

    private static final long serialVersionUID = 1L;

    public ClickHouseDialectConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case TINYINT:
                return val -> val;
            case INTEGER:
                return val -> val instanceof UnsignedShort ? ((UnsignedShort) val).intValue() : val;
            case SMALLINT:
                return val -> val instanceof UnsignedByte ? ((UnsignedByte) val).shortValue() : val;
            case VARCHAR:
                return val -> {
                    if (val instanceof String) {
                        return StringData.fromString((String) val);
                    }
                    if (val instanceof Inet4Address) {
                        return StringData.fromString(((Inet4Address) val).getHostAddress());
                    }
                    if (val instanceof Inet6Address) {
                        return StringData.fromString(((Inet6Address) val).getHostAddress());
                    }
                    if (val instanceof UUID) {
                        return StringData.fromString(val.toString());
                    }
                    return val;
                };
            case DATE:
                return val -> (int) (((LocalDate) val).toEpochDay());
            case BIGINT:
                return val ->
                        val instanceof Long
                                ? ((Long) val).longValue()
                                : val instanceof UnsignedInteger
                                        ? ((UnsignedInteger) val).longValue()
                                        : val;
            case FLOAT:
                return val -> val instanceof Float ? ((Float) val).floatValue() : val;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                return val -> {
                    if (val instanceof BigDecimal) {
                        return DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
                    }
                    if (val instanceof Number) {
                        return DecimalData.fromBigDecimal(
                                BigDecimal.valueOf(((Number) val).doubleValue()), precision, scale);
                    }
                    return val;
                };
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> {
                    if (val instanceof OffsetDateTime) {
                        return TimestampData.fromLocalDateTime(
                                ((OffsetDateTime) val).toLocalDateTime());
                    }
                    if (val instanceof LocalDateTime) {
                        return TimestampData.fromLocalDateTime((LocalDateTime) val);
                    }
                    return TimestampData.fromTimestamp((Timestamp) val);
                };
            case ARRAY:
                final LogicalType elementType = ((ArrayType) type).getElementType();
                final JdbcDeserializationConverter converter = createInternalConverter(elementType);
                return val -> {
                    // check if val is array type
                    if (val.getClass().isArray()) {
                        Object[] array = new Object[Array.getLength(val)];
                        for (int i = 0; i < Array.getLength(val); i++) {
                            Object element = converter.deserialize(Array.get(val, i));
                            array[i] = element;
                        }
                        return new GenericArrayData(array);
                    }
                    return val;
                };
            case MAP:
                final LogicalType keyType = ((MapType) type).getKeyType();
                final LogicalType valueType = ((MapType) type).getValueType();
                final JdbcDeserializationConverter keyConverter = createInternalConverter(keyType);
                final JdbcDeserializationConverter valueConverter =
                        createInternalConverter(valueType);
                return val -> {
                    // check if val is map type
                    if (val instanceof Map) {
                        Map<Object, Object> objectMap = new LinkedHashMap<>();
                        for (Object entry : ((Map) val).entrySet()) {
                            objectMap.put(
                                    keyConverter.deserialize(((Map.Entry) entry).getKey()),
                                    valueConverter.deserialize(((Map.Entry) entry).getValue()));
                        }
                        return new GenericMapData(objectMap);
                    }
                    return val;
                };
            case ROW:
                final int rowArity = ((RowType) type).getFieldCount();
                final List<JdbcDeserializationConverter> deserializationConverters =
                        ((RowType) type)
                                .getChildren().stream()
                                        .map(this::createInternalConverter)
                                        .collect(Collectors.toList());
                return val -> {
                    if (val instanceof List) {
                        GenericRowData row = new GenericRowData(rowArity);
                        for (int i = 0; i < rowArity; i++) {
                            row.setField(
                                    i,
                                    deserializationConverters
                                            .get(i)
                                            .deserialize(((List) val).get(i)));
                        }
                        return row;
                    }
                    return val;
                };
            default:
                return super.createInternalConverter(type);
        }
    }

    @Override
    protected JdbcSerializationConverter createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (val, index, statement) ->
                        statement.setTimestamp(
                                index, val.getTimestamp(index, timestampPrecision).toTimestamp());

            case ARRAY:
                return (val, index, statement) -> {
                    Object arrayVal = getValue(type, val.getArray(index));
                    statement.setObject(index, arrayVal);
                };
            case MAP:
                return (val, index, statement) -> {
                    Object mapVal = getValue(type, val.getMap(index));
                    statement.setObject(index, mapVal);
                };
            case ROW:
                final int rowArity = ((RowType) type).getFieldCount();
                return (val, index, statement) -> {
                    Object rowVal = getValue(type, val.getRow(index, rowArity));
                    statement.setObject(index, rowVal);
                };
            default:
                return super.createExternalConverter(type);
        }
    }

    @Override
    public String converterName() {
        return "ClickHouse";
    }

    // ------ Utilities ------

    static Object getValue(LogicalType type, Object data) {
        switch (type.getTypeRoot()) {
            case ARRAY:
                ArrayData arrayData = (ArrayData) data;
                ArrayData.ElementGetter innerGetter =
                        createElementGetter(((ArrayType) type).getElementType());
                return IntStream.range(0, arrayData.size())
                        .boxed()
                        .map(i -> innerGetter.getElementOrNull(arrayData, i))
                        .toArray(Object[]::new);
            case ROW:
                RowData row = (RowData) data;
                return IntStream.range(0, row.getArity())
                        .boxed()
                        .map(
                                i ->
                                        createFieldGetter(((RowType) type).getTypeAt(i), i)
                                                .getFieldOrNull(row))
                        .collect(Collectors.toList());
            case MAP:
                MapData mapData = (MapData) data;
                ArrayData.ElementGetter keyGetter =
                        createElementGetter(((MapType) type).getKeyType());
                ArrayData.ElementGetter valueGetter =
                        createElementGetter(((MapType) type).getValueType());
                Map<Object, Object> mapVal = new LinkedHashMap<>();
                IntStream.range(0, mapData.size())
                        .forEach(
                                i -> {
                                    Object key = keyGetter.getElementOrNull(mapData.keyArray(), i);
                                    Object value =
                                            valueGetter.getElementOrNull(mapData.valueArray(), i);
                                    mapVal.put(key, value);
                                });
                return mapVal;
            default:
                return data;
        }
    }

    static ArrayData.ElementGetter createElementGetter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case ARRAY:
                return (array, pos) -> getValue(type, array.getArray(pos));
            case ROW:
                return (array, pos) ->
                        getValue(type, array.getRow(pos, ((RowType) type).getFieldCount()));
            case MAP:
                return (array, pos) -> getValue(type, array.getMap(pos));
            default:
                return ArrayData.createElementGetter(type);
        }
    }

    static RowData.FieldGetter createFieldGetter(LogicalType type, int fieldPos) {
        switch (type.getTypeRoot()) {
            case ARRAY:
                return row -> getValue(type, row.getArray(fieldPos));
            case ROW:
                return row ->
                        getValue(type, row.getRow(fieldPos, ((RowType) type).getFieldCount()));
            case MAP:
                return row -> getValue(type, row.getMap(fieldPos));
            default:
                return RowData.createFieldGetter(type, fieldPos);
        }
    }
}
