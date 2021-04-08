/*
 * The MIT License
 *
 * Copyright 2020 Aberta Ltd.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package aberta.sql;

import aberta.sql.SimpleSQL.ConnectionParameters;
import aberta.sql.SimpleSQL.RowProcessor;
import aberta.sql.SimpleSQL.RowUpdater;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author chris
 */
public class Transaction {

    private Connection conn;
    private boolean uncommitedChanges = false;

    public long connectionTimeNS = 0;
    public long commitTimeNS = 0;
    public long batchUpdateTimeNS = 0;
    public long addBatchTimeNS = 0;
    public long executeQueryTimeNS = 0;
    public long updateRowTimeNS = 0;
    public long preparedStatementTimeNS = 0;

    public long numBatchUpdateCalls = 0;
    public long numAddBatchCalls = 0;
    public long numExecuteQueryCalls = 0;
    public long numUpdateRowCalls = 0;
    public long numPreparedStatementCalls = 0;

    public interface Processor {

        public boolean process(Transaction txn);
    }

    Transaction(Connection conn) throws SQLException {
        this.conn = conn;
        if (conn == null) {
            throw new IllegalArgumentException("no connection");
        }
    }

    @SuppressWarnings("UseSpecificCatch")
    public static Map<String, Object> withTransaction(ConnectionParameters params, Transaction.Processor tp) {
        if (params == null) {
            throw new IllegalArgumentException("no parameters");
        }
        if (tp == null) {
            throw new IllegalArgumentException("no transaction processor");
        }

        Map<String, Object> timings = new HashMap<>();

        long start = System.nanoTime();
        Connection conn = getConnection(params);
        long end = System.nanoTime();

        try {
            Transaction txn = new Transaction(conn);
            txn.connectionTimeNS = Math.abs(end - start);

            try {
                Boolean commit = tp.process(txn);
                if (commit) {
                    txn.commit();
                } else {
                    txn.rollback();
                }            
            } finally {
                try {
                    txn.rollback();
                } catch (SQLException ex) {
                }
            }

            timings.put("connectionTime", ((double) txn.connectionTimeNS) / (double) 1000000.0);
            timings.put("commitTime", ((double) txn.commitTimeNS) / (double) 1000000.0);
            timings.put("batchUpdateTime", ((double) txn.batchUpdateTimeNS) / (double) 1000000.0);
            timings.put("addBatchTime", ((double) txn.addBatchTimeNS) / (double) 1000000.0);
            timings.put("executeQueryTime", ((double) txn.executeQueryTimeNS) / (double) 1000000.0);
            timings.put("updateRowTime", ((double) txn.updateRowTimeNS) / (double) 1000000.0);
            timings.put("preparedStatementTime", ((double) txn.preparedStatementTimeNS) / (double) 1000000.0);

            timings.put("numBatchUpdateCalls", txn.numBatchUpdateCalls);
            timings.put("numAddBatchCalls", txn.numAddBatchCalls);
            timings.put("numExecuteQueryCalls", txn.numExecuteQueryCalls);
            timings.put("numUpdateRowCalls", txn.numUpdateRowCalls);
            timings.put("numPreparedStatementCalls", txn.numPreparedStatementCalls);

            return timings;

        } catch (Exception ex) {
            Logger.getLogger(SimpleSQL.class.getName()).
                    log(Level.SEVERE, null, ex);
            if (ex instanceof RuntimeException) {
                throw (RuntimeException)ex;
            }
            throw new RuntimeException(ex);
        } finally {
            try {
                conn.close();
            } catch (SQLException ex) {
                Logger.getLogger(SimpleSQL.class.getName()).
                        log(Level.WARNING, null, ex);
            }
        }
    }

    public void commit() throws SQLException {
        if (conn != null && uncommitedChanges) {
            long start = System.nanoTime();
            conn.commit();
            commitTimeNS += Math.abs(System.nanoTime() - start);
            uncommitedChanges = false;
        }
    }

    public void rollback() throws SQLException {
        if (conn != null && uncommitedChanges) {
            conn.rollback();
            uncommitedChanges = false;
        }
    }

    /**
     * Makes a connection to the database, run the SQL and return the first row
     *
     * @param sql the SQL SELECT query to run to fetch the data.
     * @param params an optional list of parameters to substitute for "?" in the
     * SQL
     * @return a Map representing the fetched row or null if no row was found.
     * The entries will be returned in the order that the columns in the result
     */
    public Map<String, Object> queryFirst(String sql, List<Object> params) {
        return queryFirstWithUpdater(sql, params, null);
    }

    private Map<String, Object> queryFirstWithUpdater(String sql, List<Object> params, RowUpdater updater) {

        final List<Map<String, Object>> rows = new ArrayList<>();

        executeQuery(sql, params, new RowProcessor() {
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
     * @param sql the SQL SELECT query to run to fetch the data.
     * @param params an optional list of parameters to substitute for "?" in the
     * SQL
     * @return a List of Maps representing the fetched rows. If no rows were
     * return from the database then then list will be empty. Each Map will
     * return it's entries in the order they are returned by the database query
     */
    public List<Map<String, Object>> queryAsList(String sql, List<Object> params) {

        final List<Map<String, Object>> rows = new ArrayList<>();
        query(sql, params, new RowProcessor() {
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
     * @param sql the SQL SELECT query to run to fetch the data.
     * @param params an optional list of parameters to substitute for "?" in the
     * SQL
     * @param processor the processor for each row. The process should return
     * true to continue fetching data
     */
    public void query(String sql, List<Object> params, RowProcessor processor) {
        executeQuery(sql, params, processor, null);
    }

    /**
     * Fetches the first row for the given SQL statement and calls the
     * RowUpdater to manipulate the row. If any changes are done, and if the
     * RowUpdater returns true the the row is updated.
     *
     * @param sql the SQL SELECT query to run to fetch the data.
     * @param params an optional list of parameters to substitute for "?" in the
     * SQL
     * @param updater code that will manipulate the fetched row.
     * @return the updated row
     */
    public Map<String, Object> fetchForUpdate(String sql, List<Object> params, RowUpdater updater) {

        if (updater != null) {
            return queryFirstWithUpdater(sql, params, updater);
        }
        return queryFirst(sql, params);
    }

    /**
     * @param sql
     * @param listOfParameters
     * @return the total count of updated records
     */
    public int batchUpdate(String sql, Collection<List<Object>> listOfParameters) {

        numBatchUpdateCalls++;

        try {
            long start = System.nanoTime();

            try ( PreparedStatement ps = prepareStatement(conn, sql, false)) {

                preparedStatementTimeNS += Math.abs(System.nanoTime() - start);

                for (List<Object> params : listOfParameters) {
                    int i = 1;
                    for (Object param : params) {
                        if (param instanceof InputStream) {
                            ps.setBinaryStream(i++, (InputStream) param);
                        } else {
                            ps.setObject(i++, param);
                        }
                    }

                    numAddBatchCalls++;
                    start = System.nanoTime();
                    ps.addBatch();
                    addBatchTimeNS += Math.abs(System.nanoTime() - start);
                }

                int updateCount = 0;

                start = System.nanoTime();
                int[] counts = ps.executeBatch();
                batchUpdateTimeNS += Math.abs(System.nanoTime() - start);
    
                uncommitedChanges = true;

                for (int count : counts) {
                    updateCount += count;
                }
                return updateCount;
            }
        } catch (SQLException ex) {
            Logger.getLogger(SimpleSQL.class.getName()).log(Level.SEVERE, null,
                    ex);
            throw new RuntimeException("Update failed: " + sql, ex);

        }
    }

    @SuppressWarnings("null")
    private void executeQuery(String sql, List<Object> params,
            RowProcessor processor, RowUpdater updater) {

        numExecuteQueryCalls++;

        boolean updateable = updater != null;
        try {
            conn.setReadOnly(!updateable);
            if (updateable) {
                conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            }

            long start = System.nanoTime();

            try ( PreparedStatement ps = prepareStatement(conn, sql, updateable)) {

                preparedStatementTimeNS += Math.abs(System.nanoTime() - start);

                int p = 1;
                for (Object param : params) {
                    ps.setObject(p++, param);
                }

                start = System.nanoTime();

                try ( ResultSet rs = ps.executeQuery()) {

                    executeQueryTimeNS += Math.abs(System.nanoTime() - start);

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
                                    }
                                }
                                if (differences) {
                                    numUpdateRowCalls++;
                                    start = System.nanoTime();
                                    rs.updateRow();
                                    updateRowTimeNS += Math.abs(System.nanoTime() - start);

                                    uncommitedChanges = true;                                    
                                }
                            }
                        }

                        processMore = true;
                        if (processor != null) {
                            try {
                                Object result = processor.process(row);
                                if (result != null && result instanceof Boolean) { // could be null when run in Groovy
                                    processMore = (Boolean) result;
                                }
                            } catch (Exception ex) {
                                throw new RuntimeException("Failed to process row", ex);
                            }
                        }
                    }
                }
            }
        } catch (SQLException | RuntimeException ex) {
            throw new RuntimeException("executeQuery failed for SQL: " + sql, ex);
        }
    }

    private PreparedStatement prepareStatement(Connection c, String sql,
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
            numPreparedStatementCalls++;
            
            return ps;
        } catch (SQLException ex) {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException ex2) {
                }
            }
            throw new RuntimeException("prepareStatement failed", ex);
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
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            return conn;

        } catch (SQLException | ClassNotFoundException ex) {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex2) {
                }
            }
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
