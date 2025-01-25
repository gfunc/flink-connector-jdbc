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

import org.apache.flink.connector.jdbc.testutils.tables.TableField;
import org.apache.flink.table.types.DataType;

/** ClickHouse Table field Implementation. * */
public class ClickHouseTableField extends TableField {

    protected ClickHouseTableField(
            String name, DataType dataType, TableField.DbType dbType, boolean pkField) {
        super(name, dataType, dbType, pkField);
    }

    public String asString() {
        String fieldType =
                (getDbType() != null) ? getDbType().toString() : getDataType().toString();
        return String.format("%s %s", getName(), fieldType);
    }

    /** ClickHouse DbType Implementation. */
    public static class DbType extends TableField.DbType {

        public DbType(String type) {
            super(type);
        }

        @Override
        public ClickHouseTableField.DbType notNull() {
            this.setNullable(false);
            return this;
        }

        @Override
        public String toString() {
            return String.format(getNullable() ? "Nullable(%s)" : "%s", getType());
        }
    }
}
