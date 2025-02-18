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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Jdbc implementation of a {@link CatalogTable}. */
@Internal
public class JdbcCatalogTable extends DefaultCatalogTable {
    private final String sourceType;
    private final Map<String, Optional<String>> sourceExtra;
    private final Map<String, Optional<String>> sourceDefault;

    public JdbcCatalogTable(
            String sourceType,
            Schema schema,
            @Nullable String comment,
            List<String> partitionKeys,
            Map<String, String> options,
            Map<String, Optional<String>> sourceDefault,
            Map<String, Optional<String>> sourceExtra) {
        super(schema, comment, partitionKeys, options);
        this.sourceType = sourceType;
        this.sourceDefault = sourceDefault;
        this.sourceExtra = sourceExtra;
    }

    public JdbcCatalogTable(
            String sourceType,
            Schema schema,
            @Nullable String comment,
            List<String> partitionKeys,
            Map<String, String> options,
            @Nullable Long snapshot,
            @Nullable TableDistribution distribution,
            Map<String, Optional<String>> sourceDefault,
            Map<String, Optional<String>> sourceExtra) {
        super(schema, comment, partitionKeys, options, snapshot, distribution);
        this.sourceType = sourceType;
        this.sourceDefault = sourceDefault;
        this.sourceExtra = sourceExtra;
    }

    public Map<String, Optional<String>> getSourceExtra() {
        return sourceExtra;
    }
    //    public void setSourceExtra(String columnName, @Nullable String sourceExtra) {
    //        this.sourceExtra.put(columnName, sourceExtra==null?"":sourceExtra);
    //
    //    }
    public Map<String, Optional<String>> getSourceDefault() {
        return sourceDefault;
    }
    //    public void setSourceDefault(String columnName, @Nullable String sourceDefault) {
    //        this.sourceDefault.put(columnName, sourceDefault==null?"":sourceDefault);
    //    }
    public String getSourceType() {
        return sourceType;
    }

    @Override
    public CatalogBaseTable copy() {

        return new JdbcCatalogTable(
                sourceType,
                getUnresolvedSchema(),
                getComment(),
                getPartitionKeys(),
                getOptions(),
                getSnapshot().orElse(null),
                getDistribution().orElse(null),
                sourceDefault,
                sourceExtra);
    }

    @Override
    public CatalogTable copy(Map<String, String> options) {
        return new JdbcCatalogTable(
                sourceType,
                getUnresolvedSchema(),
                getComment(),
                getPartitionKeys(),
                options,
                getSnapshot().orElse(null),
                getDistribution().orElse(null),
                sourceDefault,
                sourceExtra);
    }

    @Override
    public String toString() {
        return "JdbcCatalogTable{"
                + "schema="
                + getUnresolvedSchema()
                + ", comment='"
                + getComment()
                + "'"
                + ", distribution="
                + getDistribution()
                + ", partitionKeys="
                + getPartitionKeys()
                + ", options="
                + getOptions()
                + ", snapshot="
                + getSnapshot()
                + '}';
    }
}
