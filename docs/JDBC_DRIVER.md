# PostemberDB JDBC Driver

The standalone Maven module in jdbc-driver/ implements a JDBC 4.2 driver over PostemberDB's JSONL/1 protocol.

## Build

    mvn -f jdbc-driver/pom.xml test
    mvn -f jdbc-driver/pom.xml package

## Connect

The driver class is io.postemberdb.jdbc.PostemberDbDriver. The service-provider entry allows DriverManager to discover it automatically.

    Properties properties = new Properties();
    properties.setProperty("ssl", "true");       // default; validates the server certificate
    properties.setProperty("user", "app_reader");
    properties.setProperty("password", "...");
    Connection connection = DriverManager.getConnection(
        "jdbc:postemberdb://db.example.net:7432", properties);

For the loopback-only development server, use ssl=false and omit credentials. Remote listeners must use server-tls and authenticated accounts.

## Current support

- Connection, Statement, PreparedStatement, ResultSet, ResultSetMetaData, and basic DatabaseMetaData.
- autoCommit, BEGIN/COMMIT/ROLLBACK, forward-only read-only result sets.
- Typed scalar parameters and quote-aware client-side binding for PreparedStatement.
- TLS sockets with hostname verification by default; configure trust using the JVM trust store.
- connectTimeout and socketTimeout connection properties (milliseconds).

## Explicit limitations

- Prepared parameters are safely quoted client-side; server-side prepared-statement protocol frames are not implemented yet.
- Result sets are currently materialized in memory, not streamed.
- Generated keys, savepoints, binary SQL parameters, batch execution, callable statements, and full JDBC metadata discovery are not supported.
- The engine currently reports itself as experimental and not JDBC-compliant for production certification.
