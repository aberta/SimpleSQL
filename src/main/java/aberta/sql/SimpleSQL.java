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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Perform simple database queries without having to deal with the intricacies
 * of JDBC.
 *
 * For usage see
 * <a href="https://github.com/aberta/SimpleSQL" target="_blank">README in
 * Github</a>
 *
 * @author Chris Hopkins, Aberta Ltd.
 */
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

    /**
     * Updates a row fetched from the database
     */
    public interface RowUpdater {

        /**
         * Updates a record that has been fetched from the database
         *
         * @param row the record fetched from the database
         * @return true to update the record or false to do no updates to the
         * record
         */
        public boolean update(Map<String, Object> row);
    }

    public interface ConnectionParameters {

        public String getDriverClass();

        public String getConnectionString();

        public String getUser();

        public String getPassword();

        public Properties getProperties();
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
            final String driverClass, final String connectionString,
            final String user, final String password) {

        return connectionParameters(driverClass, connectionString, user,
                                    password, null);
    }

    /**
     * Create a new ConnectionParameters object
     *
     * @param driverClass the class name of the JDBC Driver
     * @param connectionString the JDBC connection String
     * @param user optional user ID. If the user is specified the password must
     * also be specified
     * @param password optional password
     * @param properties connection properties
     * @return connection parameters
     */
    public static ConnectionParameters connectionParameters(
            final String driverClass, final String connectionString,
            final String user, final String password,
            final Properties properties) {

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

            @Override
            public Properties getProperties() {
                return properties;
            }
        };
    }

    /**
     * Create a new ConnectionParameters from a Java Properties File. The keys
     * in the file are: driverClass, connectionString, user and password.
     *
     * @param propertiesFile the relative or absolute of the properties file
     * @return connection parameters
     */
    public static ConnectionParameters connectionParametersFromFile(
            String propertiesFile) {
        return connectionParametersFromFile(new File(propertiesFile));
    }

    /**
     * Create a new ConnectionParameters from a Java Properties File. The keys
     * in the file are: driverClass, connectionString, user and password.
     *
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

            String driverClass = props.getProperty("driverClass");
            String connectionString = props.getProperty("connectionString");
            String user = props.getProperty("user");
            String password = props.getProperty("password");

            props.remove("driverClass");
            props.remove("connectionString");
            props.remove("user");
            props.remove("password");

            return connectionParameters(driverClass, connectionString, user,
                                        password, props);

        } catch (IOException ex) {
            throw new RuntimeException(
                    "Failed to get database connection parameters from properties file "
                    + propertiesFile.getAbsolutePath(), ex);
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
            ConnectionParameters connectionParams, String sql,
            List<Object> params) {
        return queryFirstWithUpdater(connectionParams, sql, params, null);
    }

    private static Map<String, Object> queryFirstWithUpdater(
            ConnectionParameters connectionParams, String sql,
            List<Object> params, RowUpdater updater) {

        final List<Map<String, Object>> rows = new ArrayList<>();

        executeQuery(connectionParams, sql, params, new RowProcessor() {
                 @Override
                 public boolean process(Map<String, Object> row) {
                     rows.add(row);
                     return false;
                 }
             }, updater);

        if (isEmpty(rows)) {
            return null;
        }

        return rows.get(0);
    }

    /**
     * Makes a connection to the database, run the SQL and return all the rows
     * in a list. Do not use this method if the SQL could potentially return a
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
            ConnectionParameters connectionParams, String sql,
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
                             List<Object> params,
                             RowProcessor processor) {
        executeQuery(connectionParams, sql, params, processor, null);
    }

    private static void executeQuery(ConnectionParameters connectionParams,
                                     String sql, List<Object> params,
                                     RowProcessor processor, RowUpdater updater) {

        Connection c = getConnection(connectionParams);

        boolean rollbackRequired = true;
        boolean updateable = updater != null;
        try {
            c.setReadOnly(!updateable);
            if (updateable) {
                c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            }

            PreparedStatement ps = prepareStatement(c, sql, params, updateable);
            try {
                ResultSet rs = null;
                try {
                    rs = ps.executeQuery();
                    ResultSetMetaData md = rs.getMetaData();

                    Boolean processMore = true;

                    while (rs.next() && Boolean.TRUE.equals(processMore)) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        for (int i = 1; i <= md.getColumnCount(); i++) {
                            row.put(md.getColumnName(i), rs.getObject(i));
                        }

                        if (updater != null) {

                            Map<String, Object> original = new LinkedHashMap<>();
                            original.putAll(row);

                            Boolean update = updater.update(row);
                            if (update) {
                                boolean differences = false;
                                for (Map.Entry<String, Object> entry : row.
                                        entrySet()) {
                                    if (entryDifferent(entry, original)) {
                                        rs.updateObject(entry.getKey(), entry.
                                                        getValue());
                                        differences = true;
                                        rollbackRequired = false;
                                    }
                                }
                                if (differences) {
                                    rs.updateRow();
                                    c.commit();
                                }
                            }
                        }

                        processMore = (processor != null) ? processor.process(
                                row) : true;
                    }

                } catch (Exception ex) {
                    rollbackRequired = true;
                    throw new RuntimeException("query or processing failed", ex);
                } finally {
                    close((ResultSet) rs);
                }
            } finally {
                close((Statement) ps);
            }
        } catch (SQLException | RuntimeException ex) {
            rollbackRequired = true;
            throw new RuntimeException("Failed to execute SQL: " + sql, ex);
        } finally {
            close((Connection) c, rollbackRequired);
        }
    }

    /**
     * Fetches the first row for the given SQL statement and calls the
     * RowUpdater to manipulate the row. If any changes are done, and if the
     * RowUpdater returns true the the row is updated.
     *
     * @param connectionParams the connection parameters to connect to the
     * database.
     * @param sql the SQL SELECT query to run to fetch the data.
     * @param params an optional list of parameters to substitute for "?" in the
     * SQL
     * @param updater code that will manipulate the fetched row.
     * @return the updated row
     */
    public static Map<String, Object> fetchForUpdate(
            ConnectionParameters connectionParams, String sql,
            List<Object> params, RowUpdater updater) {

        if (updater != null) {
            return queryFirstWithUpdater(connectionParams, sql, params, updater);
        }
        return queryFirst(connectionParams, sql, params);
    }

    /**
     * 
     * @param connectionParams the connection parameters to connect to the
     * database.
     * @param sql
     * @param listOfParameters
     * @return the total count of updated records
     */
    public static int batchUpdate(ConnectionParameters connectionParams, String sql, Collection<List<Object>> listOfParameters) {
        Connection c = getConnection(connectionParams);
        boolean committed = false;
        try {
            PreparedStatement ps = c.prepareStatement(sql);

            for (List<Object> params: listOfParameters) {
                int i = 1;
                for (Object param : params) {
                    ps.setObject(i++, param);
                }
                ps.addBatch();
            }
            
            int updateCount = 0;
            int[] counts = ps.executeBatch();
            for (int count: counts) {
                updateCount += count;
            }

            c.commit();
            committed = true;
                        
            try {
                ps.close();
            } catch (SQLException ex) {
                Logger.getLogger(SimpleSQL.class.getName()).
                        log(Level.SEVERE, null, ex);
            }
            
            return updateCount;

        } catch (SQLException ex) {
            Logger.getLogger(SimpleSQL.class.getName()).log(Level.SEVERE, null,
                                                            ex);
            try {
                c.rollback();
            } catch (SQLException ex1) {
                Logger.getLogger(SimpleSQL.class.getName()).
                        log(Level.SEVERE, null, ex1);
            }
            throw new RuntimeException("Update failed: " + sql, ex);

        } finally {
            if (!committed) {
                try {
                    c.rollback();
                } catch (SQLException ex) {
                    Logger.getLogger(SimpleSQL.class.getName()).
                            log(Level.SEVERE, null, ex);
                }
            }
            try {
                c.close();
            } catch (SQLException ex) {
                Logger.getLogger(SimpleSQL.class.getName()).
                        log(Level.SEVERE, null, ex);
            }
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

            Properties props = params.getProperties();
            if (!anyEmpty(user, password)) {
                props.setProperty("user", user);
                props.setProperty("password", password);
            }

            conn = DriverManager.getConnection(connection, props);
            conn.setAutoCommit(false);

            return conn;

        } catch (SQLException | ClassNotFoundException ex) {
            close(conn);
            StringBuilder sb = new StringBuilder();
            sb.append(
                    "Failed to get database connection with JDBC driver class '").
                    append(className)
                    .append("', JDBC connection string '").append(connection).
                    append("'");
            if (!anyEmpty(user, password)) {
                sb.append(" and user '").append(user).append("'");
            }
            throw new RuntimeException(sb.toString(), ex);
        }
    }

    private static PreparedStatement prepareStatement(Connection c, String sql,
                                                      List<Object> params,
                                                      boolean forUpdate) {
        PreparedStatement ps = null;
        try {
            if (!forUpdate) {
                ps = c.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                                        ResultSet.CONCUR_READ_ONLY);
            } else {
                ps = c.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                                        ResultSet.CONCUR_UPDATABLE);
            }
            if (params != null) {
                int i = 1;
                for (Object obj : params) {
                    ps.setObject(i++, obj);
                }
            }
            return ps;
        } catch (Exception ex) {
            close(ps);
            throw new RuntimeException("prepareStatement failed", ex);
        }
    }

    private static void close(Connection conn) {
        close(conn, true);
    }

    private static void close(Connection conn, boolean rollbackRequired) {
        if (conn != null) {
            try {
                if (rollbackRequired) {
                    conn.rollback();
                }
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

    private static boolean entryDifferent(Map.Entry<String, Object> entry,
                                          Map<String, Object> map) {
        if (entry == null) {
            return false;
        }
        String key = entry.getKey();
        if (isEmpty(key)) {
            return false;
        }
        Object value = entry.getValue();

        if (map == null || map.isEmpty() || !map.containsKey(key)) {
            return true;
        }

        Object originalValue = map.get(key);
        if (value == originalValue) {
            return false;
        }

        if (value == null || originalValue == null) {
            return true;
        }

        return !value.equals(originalValue);
    }
}
