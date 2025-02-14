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

package org.apache.flink.connector.jdbc.adb.testutils;

import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;

import javax.sql.XADataSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** ADB metadata. */
public class AdbMetadata implements DatabaseMetadata {
    private final String username;
    private final String password;
    private final String url;
    private final String driver;
    private final String version;

    public AdbMetadata() {
        Properties properties = new Properties();
        try (InputStream input =
                getClass().getClassLoader().getResourceAsStream("test.properties")) {
            if (input == null) {
                throw new FileNotFoundException("unable to find test.properties");
            }
            properties.load(input);
            this.username = properties.getProperty("username");
            this.password = properties.getProperty("password");
            this.url = properties.getProperty("url");
            this.driver = properties.getProperty("driver");
            this.version = properties.getProperty("version");
        } catch (IOException ex) {
            throw new RuntimeException("Error reading properties file", ex);
        }
    }

    @Override
    public String getJdbcUrl() {
        return this.url;
    }

    @Override
    public String getJdbcUrlWithCredentials() {
        return String.format("%s?user=%s&password=%s", getJdbcUrl(), getUsername(), getPassword());
    }

    @Override
    public String getUsername() {
        return this.username;
    }

    @Override
    public String getPassword() {
        return this.password;
    }

    @Override
    public XADataSource buildXaDataSource() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDriverClass() {
        return this.driver;
    }

    @Override
    public String getVersion() {
        return this.version;
    }
}
