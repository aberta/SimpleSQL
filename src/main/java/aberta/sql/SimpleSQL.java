/*
MIT License

Copyright (c) 2019 Aberta Ltd.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */
package aberta.sql;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SimpleSQL {

    /**
     * Processes each row that is fetched from the database
     */
    public interface RowProcessor {

        /**
         * Process a record that has been fetched from the database.
         *
         * @param row the columns and their values. The entries are retrieved in
         * the order that the columns returned from the database
         * @return true to continue processing. Return false to stop fetching
         * more records from the database
         */
        public boolean process(Map<String, Object> row);
    }

    public interface ConnectionParameters {

        public String getDriverClass();

        public String getConnectionString();

        public String getUser();

        public String getPassword();
    }

    /**
     * Create a new ConnectionParameters object
     *
     * @param driverClass the class name of the JDBC Driver
     * @param connectionString the JDBC connection String
     * @param user optional user ID. If the user is specified the password must
     * also be specified
     * @param password optional password
     * @return connection parameters
     */
    public static ConnectionParameters connectionParameters(
            final String driverClass,
            final String connectionString,
            final String user,
            final String password) {

        return new ConnectionParameters() {
            @Override
            public String getDriverClass() {
                return driverClass;
            }

            @Override
            public String getConnectionString() {
                return connectionString;
            }

            @Override
            public String getUser() {
                return user;
            }

            @Override
            public String getPassword() {
                return password;
            }
        };
    }

    /**
     * Create a new ConnectionParameters from a Java Properties File.  The
     * keys in the file are: driverClass, connectionString, user and password.
     * @param propertiesFile the relative or absolute of the properties file
     * @return connection parameters
     */
    public static ConnectionParameters connectionParametersFromFile(
            String propertiesFile) {
        return connectionParametersFromFile(new File(propertiesFile));
    }

    /**
     * Create a new ConnectionParameters from a Java Properties File.  The
     * keys in the file are: driverClass, connectionString, user and password.
     * @param propertiesFile the file containing the keys above.
     * @return connection parameters
     */
    public static ConnectionParameters connectionParametersFromFile(
            File propertiesFile) {

        if (propertiesFile == null) {
            throw new RuntimeException("No Properties file specified");
        }
        if (!propertiesFile.exists()) {
            throw new RuntimeException("Properties file '" + propertiesFile.
                    getAbsolutePath() + "' does not exist");
        }

        Properties props = new Properties();
        try {
            props.load(new FileInputStream(propertiesFile));
            return connectionParameters(
                    props.getProperty("driverClass"),
                    props.getProperty("connectionString"),
                    props.getProperty("user"),
                    props.getProperty("password")
            );
        } catch (IOException ex) {
            throw new RuntimeException(
                    "Failed to get database connection parameters from properties file " + propertiesFile.
                    getAbsolutePath());
        }
    }

    /**
     * Makes a connection to the database, run the SQL and return the first row
     *
     * @param connectionParams the connection parameters to connect to the
     * database.
     * @param sql the SQL SELECT query to run to fetch the data.
     * @param params an optional list of parameters to substitute for "?" in the
     * SQL
     * @return a Map representing the fetched row or null if no row was found.
     * The entries will be returned in the order that the columns in the result
     */
    public static Map<String, Object> queryFirst(
            ConnectionParameters connectionParams,
            String sql,
            List<Object> params) {

        final List<Map<String, Object>> rows = new ArrayList<>();
        query(connectionParams, sql, params, new RowProcessor() {
          @Override
          public boolean process(Map<String, Object> row) {
              rows.add(row);
              return false;
          }
      });

        if (isEmpty(rows)) {
            return null;
        }

        return rows.get(0);
    }

    /**
     * Makes a connection to the database, run the SQL and return all the rows
     * in a list. Do not use this method of the SQL could potentially return a
     * large number of rows.
     *
     * @param connectionParams the connection parameters to connect to the
     * database.
     * @param sql the SQL SELECT query to run to fetch the data.
     * @param params an optional list of parameters to substitute for "?" in the
     * SQL
     * @return a List of Maps representing the fetched rows. If no rows were
     * return from the database then then list will be empty. Each Map will
     * return it's entries in the order they are returned by the database query
     */
    public static List<Map<String, Object>> queryAsList(
            ConnectionParameters connectionParams,
            String sql,
            List<Object> params) {

        final List<Map<String, Object>> rows = new ArrayList<>();
        query(connectionParams, sql, params, new RowProcessor() {
          @Override
          public boolean process(Map<String, Object> row) {
              rows.add(row);
              return true;
          }
      });
        return rows;
    }

    /**
     * Makes a connection to the database, run the SQL and processes each row
     *
     * @param connectionParams the connection parameters to connect to the
     * database.
     * @param sql the SQL SELECT query to run to fetch the data.
     * @param params an optional list of parameters to substitute for "?" in the
     * SQL
     * @param processor the processor for each row. The process should return
     * true to continue fetching data
     */
    public static void query(ConnectionParameters connectionParams, String sql,
                             List<Object> params, RowProcessor processor) {

        Connection c = getConnection(connectionParams);
        try {
            PreparedStatement ps = prepareStatement(c, sql, params);
            try {
                ResultSet rs = null;
                try {
                    rs = ps.executeQuery();
                    ResultSetMetaData md = rs.getMetaData();

                    boolean processMore = true;

                    while (rs.next() && processMore) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        for (int i = 1; i <= md.getColumnCount(); i++) {
                            row.put(md.getColumnName(i), rs.getObject(i));
                        }
                        processMore = processor.process(row);
                    }

                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                } finally {
                    close(rs);
                }
            } finally {
                close(ps);
            }
        } finally {
            close(c);
        }
    }

    private static Connection getConnection(ConnectionParameters params) {

        String className = params.getDriverClass();
        String connection = params.getConnectionString();
        String user = params.getUser();
        String password = params.getPassword();

        if (isEmpty(className)) {
            throw new RuntimeException("No JDBC Driver Class name given");
        }

        if (isEmpty(connection)) {
            throw new RuntimeException("No JDBC connection string given");
        }

        Connection conn = null;
        try {
            Class.forName(className);
            conn = anyEmpty(user, password)
                   ? DriverManager.getConnection(connection)
                   : DriverManager.getConnection(connection, user, password);
            conn.setAutoCommit(false);
            conn.setReadOnly(true);

            return conn;

        } catch (SQLException | ClassNotFoundException ex) {
            close(conn);
            StringBuilder sb = new StringBuilder();
            sb.append(
                    "Failed to get database connection with JDBC driver class '").
                    append(className)
                    .append("', JDBC connection string '")
                    .append(connection)
                    .append("'");
            if (!anyEmpty(user, password)) {
                sb.append(" and user '").append(user).append("'");
            }
            throw new RuntimeException(sb.toString(), ex);
        }
    }

    private static PreparedStatement prepareStatement(Connection c, String sql,
                                                      List<Object> params) {
        PreparedStatement ps = null;
        try {
            ps = c.prepareStatement(sql,
                                    ResultSet.TYPE_FORWARD_ONLY,
                                    ResultSet.CONCUR_READ_ONLY);
            if (params != null) {
                int i = 1;
                for (Object obj : params) {
                    ps.setObject(i++, obj);
                }
            }
            return ps;
        } catch (Exception ex) {
            close(ps);
            throw new RuntimeException(ex);
        }
    }

    private static void close(Connection conn) {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException ex1) {
            }
            try {
                conn.close();
            } catch (SQLException ex1) {
            }
        }
    }

    private static void close(Statement s) {
        if (s != null) {
            try {
                s.close();
            } catch (SQLException ex) {
            }
        }
    }

    private static void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException ex) {
            }
        }
    }

    private static boolean isEmpty(Object obj) {
        if (obj == null) {
            return true;
        }
        if (obj instanceof String) {
            return ((String) obj).trim().isEmpty();
        }
        if (obj instanceof Collection) {
            return ((Collection) obj).isEmpty();
        }
        return false;
    }

    private static boolean anyEmpty(Object... objs) {
        for (Object obj : objs) {
            if (isEmpty(obj)) {
                return true;
            }
        }
        return false;
    }
}
