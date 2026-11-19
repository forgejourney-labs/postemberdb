package io.postemberdb.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.*;
import java.lang.reflect.*;
import java.math.BigDecimal;
import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

/**
 * JDBC 4.2-compatible driver for the PostemberDB JSONL/1 transport.
 * PreparedStatement parameter values are safely quoted and bound client-side;
 * server-side prepared statement frames are a future protocol extension.
 */
public final class PostemberDbDriver implements Driver {
    private static final String PREFIX = "jdbc:postemberdb://";
    private static final ObjectMapper JSON = new ObjectMapper();
    @Override public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;
        Properties props = info == null ? new Properties() : info;
        URI uri;
        try { uri = URI.create(url.substring("jdbc:postemberdb:".length())); }
        catch (IllegalArgumentException e) { throw new SQLException("Invalid PostemberDB JDBC URL", "08001", e); }
        String host = uri.getHost();
        if (host == null || host.isBlank()) throw new SQLException("JDBC URL must contain a host", "08001");
        int port = uri.getPort() < 0 ? 7432 : uri.getPort();
        int connectTimeout = integerProperty(props, "connectTimeout", 5000);
        int socketTimeout = integerProperty(props, "socketTimeout", 30000);
        boolean tls = Boolean.parseBoolean(props.getProperty("ssl", "true"));
        String user = props.getProperty("user");
        String password = props.getProperty("password");
        if (tls && (user == null || user.isBlank() || password == null))
            throw new SQLException("TLS PostemberDB connections require user and password properties", "28000");
        Socket socket = null;
        try {
            if (tls) {
                SSLSocket ssl = (SSLSocket) SSLSocketFactory.getDefault().createSocket();
                ssl.connect(new InetSocketAddress(host, port), connectTimeout);
                ssl.setSoTimeout(socketTimeout);
                var parameters = ssl.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                ssl.setSSLParameters(parameters);
                ssl.startHandshake();
                socket = ssl;
            } else {
                socket = new Socket();
                socket.connect(new InetSocketAddress(host, port), connectTimeout);
                socket.setSoTimeout(socketTimeout);
                socket.setTcpNoDelay(true);
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), java.nio.charset.StandardCharsets.UTF_8));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), java.nio.charset.StandardCharsets.UTF_8));
            String greeting = reader.readLine();
            if (greeting == null || !greeting.startsWith("PostemberDB/")) throw new SQLException("Unexpected PostemberDB server greeting", "08001");
            writer.write("POSTEMBERDB JSONL/1\n"); writer.flush();
            checkResponse(reader.readLine(), "protocol handshake");
            if (user != null && !user.isBlank()) {
                if (password == null) throw new SQLException("JDBC password property is required when user is supplied", "28000");
                if (user.indexOf('\n') >= 0 || user.indexOf('\r') >= 0 || password.indexOf('\n') >= 0 || password.indexOf('\r') >= 0)
                    throw new SQLException("Credentials cannot contain line breaks", "28000");
                writer.write("AUTH " + user + " " + password + "\n"); writer.flush();
                checkResponse(reader.readLine(), "authentication");
            }
            ConnectionHandler handler = new ConnectionHandler(socket, reader, writer, url, user, socketTimeout);
            return proxy(Connection.class, handler);
        } catch (SQLException e) {
            closeQuietly(socket);
            throw e;
        } catch (Exception e) {
            closeQuietly(socket);
            throw new SQLException("Could not connect to PostemberDB at " + host + ":" + port, "08001", e);
        }
    }

    @Override public boolean acceptsURL(String url) { return url != null && url.startsWith(PREFIX); }
    @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[] {
            property("user", "RBAC username"), property("password", "RBAC password"),
            property("ssl", "Use TLS (default true)"), property("connectTimeout", "Connect timeout in milliseconds"),
            property("socketTimeout", "Socket read timeout in milliseconds")
        };
    }
    private static DriverPropertyInfo property(String name, String description) {
        DriverPropertyInfo p = new DriverPropertyInfo(name, null); p.description = description; return p;
    }
    @Override public int getMajorVersion() { return 0; }
    @Override public int getMinorVersion() { return 1; }
    @Override public boolean jdbcCompliant() { return false; }
    @Override public Logger getParentLogger() { return Logger.getLogger("io.postemberdb.jdbc"); }

    private static int integerProperty(Properties p, String name, int fallback) throws SQLException {
        try { return Integer.parseInt(p.getProperty(name, Integer.toString(fallback))); }
        catch (NumberFormatException e) { throw new SQLException("Invalid " + name + " property", "08001", e); }
    }
    private static void checkResponse(String line, String operation) throws SQLException {
        if (line == null) throw new SQLException("Server closed connection during " + operation, "08006");
        try {
            JsonNode node = JSON.readTree(line);
            if (!node.path("ok").asBoolean(false))
                throw new SQLException(node.path("error").asText("PostemberDB " + operation + " failed"), "28000");
        } catch (IOException e) { throw new SQLException("Invalid JSON response during " + operation, "08S01", e); }
    }
    private static void closeQuietly(Socket socket) {
        if (socket != null) try { socket.close(); } catch (IOException ignored) {}
    }
    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return (T) java.lang.reflect.Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }
    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive() || type == void.class) return null;
        if (type == boolean.class) return false;
        if (type == char.class) return '\0';
        if (type == byte.class) return (byte)0;
        if (type == short.class) return (short)0;
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == float.class) return 0f;
        if (type == double.class) return 0d;
        return null;
    }
    private static SQLException unsupported(Method method) {
        return new SQLFeatureNotSupportedException("PostemberDB JDBC does not support " + method.getName(), "0A000");
    }

    static String bindSql(String sql, SortedMap<Integer, Object> parameters) throws SQLException {
        StringBuilder out = new StringBuilder(sql.length() + parameters.size() * 8);
        int parameter = 1;
        char quote = 0;
        for (int i = 0; i < sql.length(); i++) {
            char ch = sql.charAt(i);
            if (quote != 0) {
                out.append(ch);
                if (ch == quote) {
                    if (i + 1 < sql.length() && sql.charAt(i + 1) == quote) out.append(sql.charAt(++i));
                    else quote = 0;
                }
            } else if (ch == '\'' || ch == '"') {
                quote = ch; out.append(ch);
            } else if (ch == '?') {
                if (!parameters.containsKey(parameter)) throw new SQLException("Missing SQL parameter " + parameter, "07001");
                out.append(sqlLiteral(parameters.get(parameter++)));
            } else out.append(ch);
        }
        if (parameters.size() != parameter - 1) throw new SQLException("Unexpected SQL parameter index", "07001");
        return out.toString();
    }

    private static String sqlLiteral(Object value) throws SQLException {
        if (value == null) return "NULL";
        if (value instanceof Boolean b) return b ? "TRUE" : "FALSE";
        if (value instanceof Number n) return n.toString();
        if (value instanceof byte[]) throw new SQLFeatureNotSupportedException("Binary SQL parameters are not supported by JSONL/1", "0A000");
        String text = value instanceof java.util.Date d ? new Timestamp(d.getTime()).toString() : value.toString();
        return "'" + text.replace("'", "''") + "'";
    }

    private record Query(List<String> columns, List<List<Object>> rows, int affectedRows, String message) {
        static Query empty() { return new Query(List.of(), List.of(), 0, ""); }
    }

    private static final class ConnectionHandler implements InvocationHandler {
        final Socket socket; final BufferedReader reader; final BufferedWriter writer;
        final String url; final String user; boolean autoCommit = true; boolean closed; int timeout;
        ConnectionHandler(Socket socket, BufferedReader reader, BufferedWriter writer, String url, String user, int timeout) {
            this.socket=socket; this.reader=reader; this.writer=writer; this.url=url; this.user=user; this.timeout=timeout;
        }
        synchronized Query executeSql(String sql) throws SQLException {
            ensureOpen();
            if (sql == null || sql.isBlank() || sql.indexOf('\n') >= 0 || sql.indexOf('\r') >= 0)
                throw new SQLException("SQL must be one non-empty line", "42000");
            try {
                writer.write(sql); writer.write('\n'); writer.flush();
                String line = reader.readLine();
                if (line == null) throw new SQLException("Server closed the connection", "08006");
                JsonNode root = JSON.readTree(line);
                if (!root.path("ok").asBoolean(false))
                    throw new SQLException(root.path("error").asText("PostemberDB query failed"), "42000");
                List<String> columns = new ArrayList<>();
                root.path("columns").forEach(n -> columns.add(n.asText()));
                List<List<Object>> rows = new ArrayList<>();
                for (JsonNode row : root.path("rows")) {
                    List<Object> values = new ArrayList<>();
                    row.forEach(value -> values.add(jsonValue(value)));
                    rows.add(values);
                }
                return new Query(columns, rows, root.path("affected_rows").asInt(0), root.path("message").asText(""));
            } catch (IOException e) { throw new SQLException("PostemberDB protocol I/O failed", "08S01", e); }
        }
        private void ensureOpen() throws SQLException { if (closed || socket.isClosed()) throw new SQLException("Connection is closed", "08003"); }
        @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name=method.getName();
            switch (name) {
                case "createStatement": ensureOpen(); return proxy(Statement.class, new StatementHandler(this));
                case "prepareStatement": ensureOpen(); return proxy(PreparedStatement.class, new PreparedHandler(this, (String)args[0]));
                case "nativeSQL": return args[0];
                case "setAutoCommit": {
                    boolean next=(Boolean)args[0];
                    if (autoCommit && !next) executeSql("BEGIN");
                    else if (!autoCommit && next) executeSql("COMMIT");
                    autoCommit=next; return null;
                }
                case "getAutoCommit": return autoCommit;
                case "commit": if (autoCommit) throw new SQLException("commit is invalid while autoCommit is true", "25000"); executeSql("COMMIT"); return null;
                case "rollback": if (autoCommit) throw new SQLException("rollback is invalid while autoCommit is true", "25000"); executeSql("ROLLBACK"); return null;
                case "close": if (!closed) { if (!autoCommit) try { executeSql("ROLLBACK"); } catch (SQLException ignored) {} closed=true; closeQuietly(socket); } return null;
                case "isClosed": return closed || socket.isClosed();
                case "isValid": return !closed && !socket.isClosed();
                case "getMetaData": return proxy(DatabaseMetaData.class, new MetadataHandler(this));
                case "getCatalog": return "postemberdb";
                case "setCatalog": case "setSchema": case "clearWarnings": case "setHoldability": return null;
                case "getSchema": return "public";
                case "getTransactionIsolation": return Connection.TRANSACTION_READ_COMMITTED;
                case "setTransactionIsolation": if ((Integer)args[0] != Connection.TRANSACTION_READ_COMMITTED) throw unsupported(method); return null;
                case "getHoldability": return ResultSet.CLOSE_CURSORS_AT_COMMIT;
                case "getWarnings": return null;
                case "getNetworkTimeout": return timeout;
                case "setNetworkTimeout": timeout=(Integer)args[1]; socket.setSoTimeout(timeout); return null;
                case "isReadOnly": return false;
                case "setReadOnly": return null;
                case "getTypeMap": return new HashMap<String,Class<?>>();
                case "setTypeMap": throw unsupported(method);
                case "getClientInfo": return args == null || args.length == 0 ? new Properties() : null;
                case "setClientInfo": return null;
                case "abort": closeQuietly(socket); closed=true; return null;
                case "unwrap": if (((Class<?>)args[0]).isInstance(proxy)) return proxy; throw new SQLException("Not a wrapper");
                case "isWrapperFor": return ((Class<?>)args[0]).isInstance(proxy);
                case "toString": return "PostemberDBConnection[" + url + "]";
                case "hashCode": return System.identityHashCode(proxy);
                case "equals": return proxy == args[0];
                default: throw unsupported(method);
            }
        }
    }

    private static class StatementHandler implements InvocationHandler {
        final ConnectionHandler connection; boolean closed; Query last=Query.empty(); int updateCount=-1; int queryTimeout; int fetchSize=128; int maxRows;
        StatementHandler(ConnectionHandler connection) { this.connection=connection; }
        Query execute(String sql) throws SQLException {
            if (closed) throw new SQLException("Statement is closed", "07000");
            last=connection.executeSql(sql);
            if (maxRows>0 && last.rows().size()>maxRows) last=new Query(last.columns(),last.rows().subList(0,maxRows),last.affectedRows(),last.message());
            updateCount=last.columns().isEmpty()?last.affectedRows():-1;
            return last;
        }
        @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String n=method.getName();
            switch(n) {
                case "executeQuery": return resultSet(execute((String)args[0]), (Statement)proxy);
                case "executeUpdate": case "executeLargeUpdate": {
                    Query q=execute((String)args[0]); if(!q.columns().isEmpty()) throw new SQLException("Statement produced a ResultSet", "24000");
                    return n.equals("executeLargeUpdate")?(long)q.affectedRows():q.affectedRows();
                }
                case "execute": { Query q=execute((String)args[0]); return !q.columns().isEmpty(); }
                case "getResultSet": return last.columns().isEmpty()?null:resultSet(last,(Statement)proxy);
                case "getUpdateCount": return updateCount;
                case "getLargeUpdateCount": return (long)updateCount;
                case "getConnection": return connectionProxy();
                case "close": closed=true; return null;
                case "isClosed": return closed;
                case "getWarnings": return null;
                case "clearWarnings": case "clearBatch": case "cancel": return null;
                case "setQueryTimeout": queryTimeout=(Integer)args[0]; return null;
                case "getQueryTimeout": return queryTimeout;
                case "setFetchSize": fetchSize=(Integer)args[0]; return null;
                case "getFetchSize": return fetchSize;
                case "setMaxRows": maxRows=(Integer)args[0]; return null;
                case "getMaxRows": return maxRows;
                case "getResultSetType": return ResultSet.TYPE_FORWARD_ONLY;
                case "getResultSetConcurrency": return ResultSet.CONCUR_READ_ONLY;
                case "getResultSetHoldability": return ResultSet.CLOSE_CURSORS_AT_COMMIT;
                case "getGeneratedKeys": return resultSet(Query.empty(),(Statement)proxy);
                case "getMoreResults": return false;
                case "isPoolable": return false;
                case "setPoolable": return null;
                case "closeOnCompletion": throw unsupported(method);
                case "isCloseOnCompletion": return false;
                case "unwrap": if(((Class<?>)args[0]).isInstance(proxy)) return proxy; throw new SQLException("Not a wrapper");
                case "isWrapperFor": return ((Class<?>)args[0]).isInstance(proxy);
                case "toString": return "PostemberDBStatement";
                case "hashCode": return System.identityHashCode(proxy);
                case "equals": return proxy==args[0];
                default: throw unsupported(method);
            }
        }
        Connection connectionProxy() { return proxy(Connection.class, connection); }
    }

    private static final class PreparedHandler extends StatementHandler {
        final String template; final SortedMap<Integer,Object> parameters=new TreeMap<>();
        PreparedHandler(ConnectionHandler connection,String template){super(connection);this.template=template;}
        String bound() throws SQLException { return bindSql(template,parameters); }
        @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String n=method.getName();
            if(n.equals("clearParameters")){parameters.clear();return null;}
            if(n.startsWith("set") && args!=null && args.length>=2 && args[0] instanceof Integer index) {
                if(index<1) throw new SQLException("Parameter indexes start at 1","07009");
                Object value=args[1];
                if(n.equals("setNull")) value=null;
                parameters.put(index,value);
                return null;
            }
            if(n.equals("executeQuery") && (args==null || args.length==0)) return resultSet(execute(bound()),(PreparedStatement)proxy);
            if((n.equals("executeUpdate")||n.equals("executeLargeUpdate")) && (args==null || args.length==0)) {
                Query q=execute(bound()); if(!q.columns().isEmpty()) throw new SQLException("Statement produced a ResultSet","24000");
                return n.equals("executeLargeUpdate")?(long)q.affectedRows():q.affectedRows();
            }
            if(n.equals("execute") && (args==null || args.length==0)) return !execute(bound()).columns().isEmpty();
            if(n.equals("getMetaData")) return last.columns().isEmpty()?null:metadata(last);
            if(n.equals("getParameterMetaData")) throw unsupported(method);
            return super.invoke(proxy,method,args);
        }
    }

    private static Object jsonValue(JsonNode n) {
        if(n==null || n.isNull()) return null;
        if(n.isBoolean()) return n.booleanValue();
        if(n.isIntegralNumber()) return n.longValue();
        if(n.isFloatingPointNumber()) return n.doubleValue();
        if(n.isTextual()) return n.textValue();
        return n.toString();
    }

    private static ResultSet resultSet(Query query, Statement statement) {
        return proxy(ResultSet.class,new ResultSetHandler(query,statement));
    }
    private static ResultSetMetaData metadata(Query query) {
        return proxy(ResultSetMetaData.class,new ResultSetMetadataHandler(query));
    }

    private static final class ResultSetHandler implements InvocationHandler {
        final Query query; final Statement statement; int cursor=-1; boolean closed; boolean wasNull;
        ResultSetHandler(Query query,Statement statement){this.query=query;this.statement=statement;}
        Object value(Object index) throws SQLException {
            if(closed) throw new SQLException("ResultSet is closed","24000");
            int col;
            if(index instanceof Integer i) col=i-1;
            else { String label=(String)index; col=query.columns().indexOf(label); if(col<0) col=query.columns().indexOf(label.toLowerCase(Locale.ROOT)); }
            if(col<0 || col>=query.columns().size()) throw new SQLException("Unknown ResultSet column: "+index,"S0022");
            if(cursor<0 || cursor>=query.rows().size()) throw new SQLException("Cursor is not positioned on a row","24000");
            Object v=query.rows().get(cursor).get(col); wasNull=v==null; return v;
        }
        @Override public Object invoke(Object proxy,Method method,Object[] args)throws Throwable{
            String n=method.getName();
            switch(n){
                case "next": if(closed) return false; if(cursor<query.rows().size()) cursor++; return cursor<query.rows().size();
                case "getObject": { Object v=value(args[0]); if(args.length==2 && args[1] instanceof Class<?> t && v!=null) return t.cast(v); return v; }
                case "getString": { Object v=value(args[0]); return v==null?null:String.valueOf(v); }
                case "getInt": { Object v=value(args[0]); return v==null?0:v instanceof Number x?x.intValue():Integer.parseInt(v.toString()); }
                case "getLong": { Object v=value(args[0]); return v==null?0L:v instanceof Number x?x.longValue():Long.parseLong(v.toString()); }
                case "getDouble": { Object v=value(args[0]); return v==null?0d:v instanceof Number x?x.doubleValue():Double.parseDouble(v.toString()); }
                case "getBoolean": { Object v=value(args[0]); return v==null?false:v instanceof Boolean x?x:Boolean.parseBoolean(v.toString()); }
                case "getBigDecimal": { Object v=value(args[0]); return v==null?null:new BigDecimal(v.toString()); }
                case "getDate": { Object v=value(args[0]); return v==null?null:java.sql.Date.valueOf(v.toString()); }
                case "getTimestamp": { Object v=value(args[0]); return v==null?null:Timestamp.valueOf(v.toString()); }
                case "wasNull": return wasNull;
                case "findColumn": { String label=(String)args[0]; int i=query.columns().indexOf(label); if(i<0)i=query.columns().indexOf(label.toLowerCase(Locale.ROOT)); if(i<0)throw new SQLException("Unknown column "+label); return i+1; }
                case "getMetaData": return metadata(query);
                case "getStatement": return statement;
                case "getRow": return cursor>=0 && cursor<query.rows().size()?cursor+1:0;
                case "isBeforeFirst": return !query.rows().isEmpty()&&cursor<0;
                case "isAfterLast": return !query.rows().isEmpty()&&cursor>=query.rows().size();
                case "isFirst": return cursor==0&&!query.rows().isEmpty();
                case "isLast": return cursor==query.rows().size()-1&&!query.rows().isEmpty();
                case "close": closed=true; return null;
                case "isClosed": return closed;
                case "getType": return ResultSet.TYPE_FORWARD_ONLY;
                case "getConcurrency": return ResultSet.CONCUR_READ_ONLY;
                case "getFetchDirection": return ResultSet.FETCH_FORWARD;
                case "setFetchDirection": case "setFetchSize": return null;
                case "getFetchSize": return 128;
                case "unwrap": if(((Class<?>)args[0]).isInstance(proxy))return proxy;throw new SQLException("Not a wrapper");
                case "isWrapperFor": return ((Class<?>)args[0]).isInstance(proxy);
                case "toString": return "PostemberDBResultSet";
                case "hashCode": return System.identityHashCode(proxy);
                case "equals": return proxy==args[0];
                default: throw unsupported(method);
            }
        }
    }

    private static final class ResultSetMetadataHandler implements InvocationHandler {
        final Query query; ResultSetMetadataHandler(Query query){this.query=query;}
        @Override public Object invoke(Object proxy,Method method,Object[] args)throws Throwable{
            String n=method.getName(); int index=args!=null&&args.length>0&&args[0] instanceof Integer i?i-1:0;
            if(n.equals("getColumnCount"))return query.columns().size();
            if(n.equals("getColumnLabel")||n.equals("getColumnName"))return query.columns().get(index);
            if(n.equals("getColumnType")){Object v=sample(index);return v instanceof Boolean?Types.BOOLEAN:v instanceof Number?Types.BIGINT:Types.VARCHAR;}
            if(n.equals("getColumnTypeName")){Object v=sample(index);return v instanceof Boolean?"BOOLEAN":v instanceof Number?"BIGINT":"VARCHAR";}
            if(n.equals("getColumnClassName")){Object v=sample(index);return v instanceof Boolean?Boolean.class.getName():v instanceof Number?Long.class.getName():String.class.getName();}
            if(n.equals("isNullable"))return ResultSetMetaData.columnNullableUnknown;
            if(n.equals("isReadOnly"))return true;
            if(n.equals("isWritable")||n.equals("isDefinitelyWritable")||n.equals("isAutoIncrement")||n.equals("isCurrency"))return false;
            if(n.equals("isCaseSensitive")||n.equals("isSearchable")||n.equals("isSigned"))return true;
            if(n.equals("getPrecision")||n.equals("getScale")||n.equals("getColumnDisplaySize"))return 0;
            if(n.equals("getSchemaName")||n.equals("getTableName")||n.equals("getCatalogName"))return "";
            if(n.equals("unwrap"))throw new SQLException("Not a wrapper");
            if(n.equals("isWrapperFor"))return false;
            if(n.equals("toString"))return "PostemberDBResultSetMetaData";
            return defaultValue(method.getReturnType());
        }
        Object sample(int column){return query.rows().isEmpty()?null:query.rows().get(0).get(column);}
    }

    private static final class MetadataHandler implements InvocationHandler {
        final ConnectionHandler connection; MetadataHandler(ConnectionHandler c){connection=c;}
        @Override public Object invoke(Object proxy,Method method,Object[] args)throws Throwable{
            return switch(method.getName()){
                case "getURL" -> connection.url;
                case "getUserName" -> connection.user==null?"":connection.user;
                case "getDriverName" -> "PostemberDB JDBC Driver";
                case "getDriverVersion" -> "0.1.0";
                case "getDatabaseProductName" -> "PostemberDB";
                case "getDatabaseProductVersion" -> "0.1.0";
                case "getConnection" -> proxy(Connection.class,connection);
                case "supportsTransactions" -> true;
                case "supportsBatchUpdates" -> false;
                case "supportsSavepoints","supportsMultipleResultSets" -> false;
                case "getDefaultTransactionIsolation" -> Connection.TRANSACTION_READ_COMMITTED;
                case "getIdentifierQuoteString" -> "`";
                case "getCatalogSeparator" -> ".";
                case "getCatalogTerm" -> "database";
                case "getSchemaTerm" -> "schema";
                case "getTableTerm" -> "table";
                case "getSearchStringEscape" -> "\\\\";
                case "getSQLStateType" -> DatabaseMetaData.sqlStateSQL;
                case "getDriverMajorVersion" -> 0;
                case "getDriverMinorVersion" -> 1;
                case "getDatabaseMajorVersion" -> 0;
                case "getDatabaseMinorVersion" -> 1;
                case "getJDBCMajorVersion" -> 4;
                case "getJDBCMinorVersion" -> 2;
                case "getMaxConnections","getMaxStatements","getMaxTablesInSelect" -> 0;
                case "isReadOnly" -> false;
                case "allProceduresAreCallable","allTablesAreSelectable" -> false;
                case "unwrap" -> { if(((Class<?>)args[0]).isInstance(proxy))yield proxy; throw new SQLException("Not a wrapper"); }
                case "isWrapperFor" -> ((Class<?>)args[0]).isInstance(proxy);
                case "toString" -> "PostemberDBDatabaseMetaData";
                default -> defaultValue(method.getReturnType());
            };
        }
    }
}