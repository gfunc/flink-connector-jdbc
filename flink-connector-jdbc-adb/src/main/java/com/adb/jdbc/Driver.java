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

package com.adb.jdbc;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

/** Driver implementation for ADB. */
public class Driver extends com.mysql.cj.jdbc.NonRegisteringDriver {
    static {
        try {
            java.sql.DriverManager.registerDriver(new com.adb.jdbc.Driver());
        } catch (SQLException E) {
            throw new RuntimeException("Can't register driver!");
        }
    }

    public Driver() throws SQLException {}

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        url = url.replaceFirst("jdbc:adb:", "jdbc:mysql:");
        return super.acceptsURL(url);
    }

    @Override
    public java.sql.Connection connect(String url, Properties info) throws SQLException {
        url = url.replaceFirst("jdbc:adb:", "jdbc:mysql:");
        return super.connect(url, info);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        url = url.replaceFirst("jdbc:adb:", "jdbc:mysql:");
        return super.getPropertyInfo(url, info);
    }
}
