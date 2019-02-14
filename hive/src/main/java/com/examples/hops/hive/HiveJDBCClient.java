/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.examples.hops.hive;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

/**
 * Demo class showing how to connect to Hive on Hops using a secure JDBC connection.
 * It initiates a secure connection to hive server and executes a query. The database and query used are
 * taken from Hops documentation. More detailed documentation on using JDBC with HIVE is available here:
 * https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-JDBCClientSampleCode
 * <p>
 */
public class HiveJDBCClient {

    private static final Logger LOG = LogManager.getLogger(HiveJDBCClient.class);
    private static final String HIVE_CREDENTIALS = "hive_credentials.properties";

    //Hive credentials property names
    private static final String HIVE_URL = "hive_url";
    private static final String CONNECTION_TYPE = "connection_type";
    private static final String DB_NAME = "dbname";
    private static final String TRUSTSTORE_PATH = "truststore_path";
    private static final String TRUSTSTORE_PW = "truststore_pw";
    private static final String KEYSTORE_PATH = "keystore_path";
    private static final String KEYSTORE_PW = "keystore_pw";
    private static final String HOPSWORKS_USERNAME = "hopsworks_username";
    private static final String HOPSWORKS_PW = "hopsworks_pw";

    private enum ConnectionType {
        USERNAME,
        KEYSTORE
    }

    public static void main(String[] args) throws SQLException, IOException {

        if (args == null || args.length == 0) {
            LOG.warn("Nor input arguments found. Please provide location of input raw data. For example " +
                    "<Projects/hivedemo/Resources/rawdata>");
            System.exit(1);
        }
        Properties hiveCredentials = readHiveCredentials(HIVE_CREDENTIALS);
        String rawdata = args[0];
        ConnectionType connectionType = ConnectionType.valueOf(hiveCredentials.getProperty(CONNECTION_TYPE));

        LOG.info(hiveCredentials);

        try (Connection conn = HiveJDBCClient.getHiveJDBCConnection(connectionType)) {

            //Set hive/tez properties
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict;");
            }

            LOG.info("Drop external table if exists");
            //Drop table
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("drop table if exists sales");
            }

            LOG.info("Create external table...");
            //Create external table

            String query = "create external table sales(" + "  street string," +
                    "  city string," +
                    "  zip int," +
                    "  state string," +
                    "  beds int," +
                    "  baths int," +
                    "  sq__ft float," +
                    "  sales_type string," +
                    "  sale_date string," +
                    "  price float," +
                    "  latitude float," +
                    "  longitude float)" +
                    "  ROW FORMAT DELIMITED" +
                    "  FIELDS TERMINATED BY ','" +
                    "  LOCATION ?";

            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, rawdata);
                stmt.execute();
            }


            LOG.info("Drop orc table if exists");
            //Drop table
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("drop table if exists orc_table");
            }

            LOG.info("Creating Hive table...");
            //Create hive table
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("create table orc_table (" +
                        "  street string," +
                        "  city string," +
                        "  zip int," +
                        "  state string," +
                        "  beds int," +
                        "  baths int," +
                        "  sq__ft float," +
                        "  sales_type string," +
                        "  sale_date string," +
                        "  price float," +
                        "  latitude float," +
                        "  longitude float)" +
                        "  STORED AS ORC");
            }

            LOG.info("Inserting data into Hive table from external one...");

            //Insert data from external table into hive one
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("insert overwrite table orc_table select * from sales");
            }

            LOG.info("Selecting average price per city...");
            //Select and display data
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rst = stmt.executeQuery("select city, avg(price) as price from sales group by city")) {
                    LOG.info("City \t Price");
                    while (rst.next()) {

                        String city = rst.getString(1);
                        String price = rst.getString(2);

                        if (city != null && !city.isEmpty()) {
                            LOG.info(city + "\t" + price);
                        }
                    }
                }
            }
        }

        LOG.info("Exiting...");

    }

    /**
     * Initializes a JDBC connection to Hopsworks Hive server by reading credentials from properties file.
     *
     * @param connectionType the connection type depending the parameters to read from the file
     * @return
     * @throws SQLException
     * @throws IOException
     */
    private static Connection getHiveJDBCConnection(ConnectionType connectionType) throws SQLException, IOException {
        switch (connectionType) {
            case USERNAME:
                return getHiveJDBCConnection();
            case KEYSTORE:
                return getHiveJDBCConnectionWithKeyStore();
            default:
                throw new IOException("Connection Type unknown");
        }
    }

    /**
     * Initializes a JDBC connection to Hopsworks Hive server by reading credentials from properties file.
     *
     * @return Connection Hive JDBC Connection
     * @throws SQLException
     * @throws IOException
     */
    private static Connection getHiveJDBCConnection() throws SQLException, IOException {
        LOG.info("Reading hive credentials from properties file");
        Properties hiveCredentials = readHiveCredentials(HIVE_CREDENTIALS);
        LOG.info("Establishing connection to Hive server at:" + hiveCredentials.getProperty(HIVE_URL));

        Connection conn = DriverManager.getConnection(hiveCredentials.getProperty(HIVE_URL) + "/"
                        + hiveCredentials.getProperty(DB_NAME) + ";auth=noSasl;ssl=true;"
                        + "sslTrustStore=" + hiveCredentials.getProperty(TRUSTSTORE_PATH)
                        + ";trustStorePassword=" + hiveCredentials.getProperty(TRUSTSTORE_PW),
                hiveCredentials.getProperty(HOPSWORKS_USERNAME),
                hiveCredentials.getProperty(HOPSWORKS_PW));
        LOG.info("Connection established!");
        return conn;
    }

    /**
     * Initializes a JDBC connection to Hopsworks Hive server by reading credentials from properties file. <br>
     * The difference with getHiveJDBCConnection is in the connection parameters used.<br>
     *
     * @return Connection Hive JDBC Connection
     * @throws SQLException
     * @throws IOException
     */
    private static Connection getHiveJDBCConnectionWithKeyStore() throws SQLException, IOException {
        LOG.info("Reading hive credentials from properties file");
        Properties hiveCredentials = readHiveCredentials(HIVE_CREDENTIALS);
        LOG.info("Establishing connection to Hive server at:" + hiveCredentials.getProperty(HIVE_URL));

        Connection conn = DriverManager.getConnection(hiveCredentials.getProperty(HIVE_URL) + "/"
                + hiveCredentials.getProperty(DB_NAME)
                + ";auth=noSasl;ssl=true;twoWay=true"
                + ";sslTrustStore=" + hiveCredentials.getProperty(TRUSTSTORE_PATH)
                + ";trustStorePassword=" + hiveCredentials.getProperty(TRUSTSTORE_PW)
                + ";sslKeyStore=" + hiveCredentials.getProperty(KEYSTORE_PATH)
                + ";keyStorePassword=" + hiveCredentials.getProperty(KEYSTORE_PW)
        );
        LOG.info("Connection established!");
        return conn;
    }


    private static Properties readHiveCredentials(String path) throws IOException {
        InputStream stream = HiveJDBCClient.class.getClassLoader().getResourceAsStream("./com/examples/hops/hive/" + path);
        if (stream == null) {
            throw new IOException("No ." + HIVE_CREDENTIALS + " properties file found");
        }
        Properties props = new Properties();
        props.load(stream);
        return props;
    }

}