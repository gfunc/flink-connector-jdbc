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

package org.apache.flink.connector.jdbc.utils;

import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.apache.flink.connector.jdbc.utils.JdbcTypeUtil.logicalTypeToSqlType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Testing the type conversions from Flink to SQL types. */
class JdbcTypeUtilTest {

    @Test
    void testTypeConversions() {
        assertThat(logicalTypeToSqlType(LogicalTypeRoot.INTEGER)).isEqualTo(Types.INTEGER);
        testUnsupportedType(LogicalTypeRoot.RAW);
        testUnsupportedType(LogicalTypeRoot.STRUCTURED_TYPE);
    }

    private static void testUnsupportedType(LogicalTypeRoot logicalTypeRoot) {
        assertThatThrownBy(() -> logicalTypeToSqlType(logicalTypeRoot))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
