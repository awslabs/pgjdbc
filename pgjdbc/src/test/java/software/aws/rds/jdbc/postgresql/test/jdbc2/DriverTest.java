package software.aws.rds.jdbc.postgresql.test.jdbc2;

import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.postgresql.Driver;
import org.postgresql.PGEnvironment;
import org.postgresql.PGProperty;
import org.postgresql.test.TestUtil;
import org.postgresql.util.URLCoder;

import software.aws.rds.jdbc.postgresql.test.AwsTestUtil;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;
import uk.org.webcompere.systemstubs.properties.SystemProperties;
import uk.org.webcompere.systemstubs.resource.Resources;

import java.io.PrintStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;

@ExtendWith(SystemStubsExtension.class)
public class DriverTest {

  /*
   * This tests the acceptsURL() method with a couple of well and poorly formed jdbc urls.
   */
  @Test
  public void testAcceptsURL() throws Exception {
    AwsTestUtil.initDriver(); // Set up log levels, etc.

    // Load the driver (note clients should never do it this way!)
    software.aws.rds.jdbc.postgresql.Driver drv = new software.aws.rds.jdbc.postgresql.Driver();
    assertNotNull(drv);

    // These are always correct
    verifyUrl(drv, "jdbc:postgresql:aws:test", "localhost", "5432", "test");
    verifyUrl(drv, "jdbc:postgresql:aws://localhost/test", "localhost", "5432", "test");
    verifyUrl(drv, "jdbc:postgresql:aws://localhost,locahost2/test", "localhost,locahost2", "5432,5432", "test");
    verifyUrl(drv, "jdbc:postgresql:aws://localhost:5433,locahost2:5434/test", "localhost,locahost2", "5433,5434", "test");
    verifyUrl(drv, "jdbc:postgresql:aws://[::1]:5433,:5434,[::1]/test", "[::1],localhost,[::1]", "5433,5434,5432", "test");
    verifyUrl(drv, "jdbc:postgresql:aws://localhost/test?port=8888", "localhost", "8888", "test");
    verifyUrl(drv, "jdbc:postgresql:aws://localhost:5432/test", "localhost", "5432", "test");
    verifyUrl(drv, "jdbc:postgresql:aws://localhost:5432/test?dbname=test2", "localhost", "5432", "test2");
    verifyUrl(drv, "jdbc:postgresql:aws://127.0.0.1/anydbname", "127.0.0.1", "5432", "anydbname");
    verifyUrl(drv, "jdbc:postgresql:aws://127.0.0.1:5433/hidden", "127.0.0.1", "5433", "hidden");
    verifyUrl(drv, "jdbc:postgresql:aws://127.0.0.1:5433/hidden?port=7777", "127.0.0.1", "7777", "hidden");
    verifyUrl(drv, "jdbc:postgresql:aws://[::1]:5740/db", "[::1]", "5740", "db");
    verifyUrl(drv, "jdbc:postgresql:aws://[::1]:5740/my%20data%23base%251?loggerFile=C%3A%5Cdir%5Cfile.log", "[::1]", "5740", "my data#base%1");

    // tests for service syntax
    URL urlFileProps = getClass().getResource("/pg_service/pgservicefileProps.conf");
    assertNotNull(urlFileProps);
    Resources.with(
        new SystemProperties(PGEnvironment.ORG_POSTGRESQL_PGSERVICEFILE.getName(), urlFileProps.getFile())
    ).execute(() -> {
      // correct cases
      verifyUrl(drv, "jdbc:postgresql:aws://?service=driverTestService1", "test-host1", "5444", "testdb1");
      verifyUrl(drv, "jdbc:postgresql:aws://?service=driverTestService1&host=other-host", "other-host", "5444", "testdb1");
      verifyUrl(drv, "jdbc:postgresql:aws:///?service=driverTestService1", "test-host1", "5444", "testdb1");
      verifyUrl(drv, "jdbc:postgresql:aws:///?service=driverTestService1&port=3333&dbname=other-db", "test-host1", "3333", "other-db");
      verifyUrl(drv, "jdbc:postgresql:aws://localhost:5432/test?service=driverTestService1", "localhost", "5432", "test");
      verifyUrl(drv, "jdbc:postgresql:aws://localhost:5432/test?port=7777&dbname=other-db&service=driverTestService1", "localhost", "7777", "other-db");
      verifyUrl(drv, "jdbc:postgresql:aws://[::1]:5740/?service=driverTestService1", "[::1]", "5740", "testdb1");
      verifyUrl(drv, "jdbc:postgresql:aws://:5740/?service=driverTestService1", "localhost", "5740", "testdb1");
      verifyUrl(drv, "jdbc:postgresql:aws://[::1]/?service=driverTestService1", "[::1]", "5432", "testdb1");
      verifyUrl(drv, "jdbc:postgresql:aws://localhost/?service=driverTestService2", "localhost", "5432", "testdb1");
      // fail cases
      assertFalse(drv.acceptsURL("jdbc:postgresql:aws://?service=driverTestService2"));
    });

    // Badly formatted url's
    assertFalse(drv.acceptsURL("jdbc:postgres:aws:test"));
    assertFalse(drv.acceptsURL("jdbc:postgresql:aws:/test"));
    assertFalse(drv.acceptsURL("jdbc:postgresql:aws:////"));
    assertFalse(drv.acceptsURL("jdbc:postgresql:aws:///?service=my data#base%1"));
    assertFalse(drv.acceptsURL("jdbc:postgresql:aws://[::1]:5740/my data#base%1"));
    assertFalse(drv.acceptsURL("jdbc:postgresql:aws://localhost/dbname?loggerFile=C%3A%5Cdir%5Cfile.%log"));
    assertFalse(drv.acceptsURL("postgresql:aws:test"));
    assertFalse(drv.acceptsURL("aws:db"));
    assertFalse(drv.acceptsURL("jdbc:postgresql:aws://localhost:5432a/test"));
    assertFalse(drv.acceptsURL("jdbc:postgresql:aws://localhost:500000/test"));
    assertFalse(drv.acceptsURL("jdbc:postgresql:aws://localhost:0/test"));
    assertFalse(drv.acceptsURL("jdbc:postgresql:aws://localhost:-2/test"));

    // failover urls
    verifyUrl(drv, "jdbc:postgresql:aws://localhost,127.0.0.1:5432/test", "localhost,127.0.0.1",
        "5432,5432", "test");
    verifyUrl(drv, "jdbc:postgresql:aws://localhost:5433,127.0.0.1:5432/test", "localhost,127.0.0.1",
        "5433,5432", "test");
    verifyUrl(drv, "jdbc:postgresql:aws://[::1],[::1]:5432/db", "[::1],[::1]", "5432,5432", "db");
    verifyUrl(drv, "jdbc:postgresql:aws://[::1]:5740,127.0.0.1:5432/db", "[::1],127.0.0.1", "5740,5432",
        "db");
  }

  private void verifyUrl(Driver drv, String url, String hosts, String ports, String dbName)
      throws Exception {
    assertTrue(url, drv.acceptsURL(url));
    Method parseMethod =
        drv.getClass().getDeclaredMethod("parseURL", String.class, Properties.class);
    parseMethod.setAccessible(true);
    Properties p = (Properties) parseMethod.invoke(drv, url, null);
    assertEquals(url, dbName, p.getProperty(PGProperty.PG_DBNAME.getName()));
    assertEquals(url, hosts, p.getProperty(PGProperty.PG_HOST.getName()));
    assertEquals(url, ports, p.getProperty(PGProperty.PG_PORT.getName()));
  }

  /**
   * Tests the connect method by connecting to the test database.
   */
  @Test
  public void testConnect() throws Exception {
    AwsTestUtil.initDriver(); // Set up log levels, etc.

    // Test with the url, username & password
    Connection con =
        DriverManager.getConnection(AwsTestUtil.getURL(), TestUtil.getUser(), TestUtil.getPassword());
    assertNotNull(con);
    con.close();

    // Test with the username in the url
    con = DriverManager.getConnection(
        AwsTestUtil.getURL()
            + "&user=" + URLCoder.encode(TestUtil.getUser())
            + "&password=" + URLCoder.encode(TestUtil.getPassword()));
    assertNotNull(con);
    con.close();

    // Test with failover url
  }

  /**
   * Tests the connect method by connecting to the test database.
   */
  @Test
  public void testConnectService() throws Exception {
    AwsTestUtil.initDriver(); // Set up log levels, etc.
    String wrongPort = "65536";

    // Create temporary pg_service.conf file
    Path tempDirWithPrefix = Files.createTempDirectory("junit");
    Path tempFile = Files.createTempFile(tempDirWithPrefix, "pg_service", "conf");
    try {
      // Write service section
      String testService1 = "testService1"; // with correct port
      String testService2 = "testService2"; // with wrong port
      try (PrintStream ps = new PrintStream(Files.newOutputStream(tempFile))) {
        ps.printf("[%s]%nhost=%s%nport=%s%ndbname=%s%nuser=%s%npassword=%s%n", testService1, TestUtil.getServer(), TestUtil.getPort(), TestUtil.getDatabase(), TestUtil.getUser(), TestUtil.getPassword());
        ps.printf("[%s]%nhost=%s%nport=%s%ndbname=%s%nuser=%s%npassword=%s%n", testService2, TestUtil.getServer(), wrongPort, TestUtil.getDatabase(), TestUtil.getUser(), TestUtil.getPassword());
      }
      // consume service
      Resources.with(
          new EnvironmentVariables(PGEnvironment.PGSERVICEFILE.getName(), tempFile.toString(), PGEnvironment.PGSYSCONFDIR.getName(), ""),
          new SystemProperties(PGEnvironment.ORG_POSTGRESQL_PGSERVICEFILE.getName(), "", "user.home", "/tmp/dir-non-existent")
      ).execute(() -> {
        //
        // testing that properties overriding priority is correct (POSITIVE cases)
        //
        // service=correct port
        Connection con = DriverManager.getConnection(String.format("jdbc:postgresql:aws://?service=%s", testService1));
        assertNotNull(con);
        con.close();
        // service=wrong port; Properties=correct port
        Properties info = new Properties();
        info.setProperty("PGPORT", String.valueOf(TestUtil.getPort()));
        con = DriverManager.getConnection(String.format("jdbc:postgresql:aws://?service=%s", testService2), info);
        assertNotNull(con);
        con.close();
        // service=wrong port; Properties=wrong port; URL port=correct
        info.setProperty("PGPORT", wrongPort);
        con = DriverManager.getConnection(String.format("jdbc:postgresql:aws://:%s/?service=%s", TestUtil.getPort(), testService2), info);
        assertNotNull(con);
        con.close();
        // service=wrong port; Properties=wrong port; URL port=wrong; URL argument=correct port
        con = DriverManager.getConnection(String.format("jdbc:postgresql:aws://:%s/?service=%s&port=%s", wrongPort, testService2, TestUtil.getPort()), info);
        assertNotNull(con);
        con.close();

        //
        // testing that properties overriding priority is correct (NEGATIVE cases)
        //
        // service=wrong port
        try {
          con = DriverManager.getConnection(String.format("jdbc:postgresql:aws://?service=%s", testService2));
          fail("Expected an SQLException because port is out of range");
        } catch (SQLException e) {
          // Expected exception.
        }
        // service=correct port; Properties=wrong port
        info.setProperty("PGPORT", wrongPort);
        try {
          con = DriverManager.getConnection(String.format("jdbc:postgresql:aws://?service=%s", testService1), info);
          fail("Expected an SQLException because port is out of range");
        } catch (SQLException e) {
          // Expected exception.
        }
        // service=correct port; Properties=correct port; URL port=wrong
        info.setProperty("PGPORT", String.valueOf(TestUtil.getPort()));
        try {
          con = DriverManager.getConnection(String.format("jdbc:postgresql:aws://:%s/?service=%s", wrongPort, testService1), info);
          fail("Expected an SQLException because port is out of range");
        } catch (SQLException e) {
          // Expected exception.
        }
        // service=correct port; Properties=correct port; URL port=correct; URL argument=wrong port
        try {
          con = DriverManager.getConnection(String.format("jdbc:postgresql:aws://:%s/?service=%s&port=%s", TestUtil.getPort(), testService1, wrongPort), info);
          fail("Expected an SQLException because port is out of range");
        } catch (SQLException e) {
          // Expected exception.
        }
      });
    } finally {
      // cleanup
      Files.delete(tempFile);
      Files.delete(tempDirWithPrefix);
    }
  }

  @Test
  public void testRegistration() throws Exception {
    AwsTestUtil.initDriver();

    // Driver is initially registered because it is automatically done when class is loaded
    assertTrue(software.aws.rds.jdbc.postgresql.Driver.isRegistered());

    ArrayList<java.sql.Driver> drivers = Collections.list(DriverManager.getDrivers());
    searchInstanceOf: {

      for (java.sql.Driver driver : drivers) {
        if (driver instanceof software.aws.rds.jdbc.postgresql.Driver) {
          break searchInstanceOf;
        }
      }
      fail("Driver has not been found in DriverManager's list but it should be registered");
    }

    // Deregister the driver
    software.aws.rds.jdbc.postgresql.Driver.deregister();
    assertFalse(software.aws.rds.jdbc.postgresql.Driver.isRegistered());

    drivers = Collections.list(DriverManager.getDrivers());
    for (java.sql.Driver driver : drivers) {
      if (driver instanceof software.aws.rds.jdbc.postgresql.Driver) {
        fail("Driver should be deregistered but it is still present in DriverManager's list");
      }
    }

    // register again the driver
    software.aws.rds.jdbc.postgresql.Driver.register();
    assertTrue(software.aws.rds.jdbc.postgresql.Driver.isRegistered());

    drivers = Collections.list(DriverManager.getDrivers());
    for (java.sql.Driver driver : drivers) {
      if (driver instanceof software.aws.rds.jdbc.postgresql.Driver) {
        return;
      }
    }
    fail("Driver has not been found in DriverManager's list but it should be registered");
  }

}
