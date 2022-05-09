/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.test;

import org.postgresql.PGProperty;
import org.postgresql.core.Version;
import org.postgresql.test.TestUtil;

import org.junit.Assume;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Utility class for JDBC tests.
 */
public class AwsTestUtil {

  /*
   * Returns the Test database JDBC URL
   */
  public static String getURL() {
    return getURL(TestUtil.getServer(), + TestUtil.getPort());
  }

  public static String getURL(String server, int port) {
    return getURL(server + ":" + port, TestUtil.getDatabase());
  }

  public static String getURL(String hostport, String database) {
    String logLevel = "";
    if (TestUtil.getLogLevel() != null && !TestUtil.getLogLevel().equals("")) {
      logLevel = "&loggerLevel=" + TestUtil.getLogLevel();
    }

    String logFile = "";
    if (TestUtil.getLogFile() != null && !TestUtil.getLogFile().equals("")) {
      logFile = "&loggerFile=" + TestUtil.getLogFile();
    }

    String protocolVersion = "";
    if (TestUtil.getProtocolVersion() != 0) {
      protocolVersion = "&protocolVersion=" + TestUtil.getProtocolVersion();
    }

    String options = "";
    if (TestUtil.getOptions() != null) {
      options = "&options=" + TestUtil.getOptions();
    }

    String binaryTransfer = "";
    if (TestUtil.getBinaryTransfer() != null && !TestUtil.getBinaryTransfer().equals("")) {
      binaryTransfer = "&binaryTransfer=" + TestUtil.getBinaryTransfer();
    }

    String receiveBufferSize = "";
    if (TestUtil.getReceiveBufferSize() != -1) {
      receiveBufferSize = "&receiveBufferSize=" + TestUtil.getReceiveBufferSize();
    }

    String sendBufferSize = "";
    if (TestUtil.getSendBufferSize() != -1) {
      sendBufferSize = "&sendBufferSize=" + TestUtil.getSendBufferSize();
    }

    String ssl = "";
    if (TestUtil.getSSL() != null) {
      ssl = "&ssl=" + TestUtil.getSSL();
    }

    return "jdbc:postgresql:aws://"
        + hostport + "/"
        + database
        + "?ApplicationName=Driver Tests"
        + logLevel
        + logFile
        + protocolVersion
        + options
        + binaryTransfer
        + receiveBufferSize
        + sendBufferSize
        + ssl;
  }

  static {
    try {
      initDriver();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Unable to initialize driver", e);
    }
  }

  public static void initDriver() throws SQLException {
    TestUtil.initDriver();

    if (!software.aws.rds.jdbc.postgresql.Driver.isRegistered()) {
      software.aws.rds.jdbc.postgresql.Driver.register();
    }
  }

  /**
   * Get a connection using a privileged user mostly for tests that the ability to load C functions
   * now as of 4/14.
   *
   * @return connection using a privileged user mostly for tests that the ability to load C
   *         functions now as of 4/14
   */
  public static Connection openPrivilegedDB() throws SQLException {
    TestUtil.initDriver();
    Properties properties = new Properties();

    PGProperty.GSS_ENC_MODE.set(properties,TestUtil.getGSSEncMode().value);
    properties.setProperty("user", TestUtil.getPrivilegedUser());
    properties.setProperty("password", TestUtil.getPrivilegedPassword());
    properties.setProperty("options", "-c synchronous_commit=on");
    return DriverManager.getConnection(getURL(), properties);

  }

  public static Connection openReplicationConnection() throws Exception {
    Properties properties = new Properties();
    PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "9.4");
    PGProperty.PROTOCOL_VERSION.set(properties, "3");
    PGProperty.REPLICATION.set(properties, "database");
    //Only simple query protocol available for replication connection
    PGProperty.PREFER_QUERY_MODE.set(properties, "simple");
    properties.setProperty("username", TestUtil.getPrivilegedUser());
    properties.setProperty("password", TestUtil.getPrivilegedPassword());
    properties.setProperty("options", "-c synchronous_commit=on");
    return openDB(properties);
  }

  /**
   * Helper - opens a connection.
   *
   * @return connection
   */
  public static Connection openDB() throws SQLException {
    return openDB(new Properties());
  }

  /*
   * Helper - opens a connection with the allowance for passing additional parameters, like
   * "compatible".
   */
  public static Connection openDB(Properties props) throws SQLException {
    initDriver();

    // Allow properties to override the user name.
    String user = props.getProperty("username");
    if (user == null) {
      user = TestUtil.getUser();
    }
    if (user == null) {
      throw new IllegalArgumentException(
          "user name is not specified. Please specify 'username' property via -D or build.properties");
    }
    props.setProperty("user", user);

    // Allow properties to override the password.
    String password = props.getProperty("password");
    if (password == null) {
      password = TestUtil.getPassword() != null ? TestUtil.getPassword() : "";
    }
    props.setProperty("password", password);

    String sslPassword = TestUtil.getSslPassword();
    if (sslPassword != null) {
      PGProperty.SSL_PASSWORD.set(props, sslPassword);
    }

    if (!props.containsKey(PGProperty.PREPARE_THRESHOLD.getName())) {
      PGProperty.PREPARE_THRESHOLD.set(props, TestUtil.getPrepareThreshold());
    }
    if (!props.containsKey(PGProperty.PREFER_QUERY_MODE.getName())) {
      String value = System.getProperty(PGProperty.PREFER_QUERY_MODE.getName());
      if (value != null) {
        props.put(PGProperty.PREFER_QUERY_MODE.getName(), value);
      }
    }
    // Enable Base4 tests to override host,port,database
    String hostport = props.getProperty(TestUtil.SERVER_HOST_PORT_PROP, TestUtil.getServer() + ":" + TestUtil.getPort());
    String database = props.getProperty(TestUtil.DATABASE_PROP, TestUtil.getDatabase());

    // Set GSSEncMode for tests only in the case the property is already missing
    if (PGProperty.GSS_ENC_MODE.getSetString(props) == null) {
      PGProperty.GSS_ENC_MODE.set(props, TestUtil.getGSSEncMode().value);
    }

    return DriverManager.getConnection(getURL(hostport, database), props);
  }

  public static void assumeHaveMinimumServerVersion(Version version)
      throws SQLException {
    try (Connection conn = openPrivilegedDB()) {
      Assume.assumeTrue(TestUtil.haveMinimumServerVersion(conn, version));
    }
  }

}
