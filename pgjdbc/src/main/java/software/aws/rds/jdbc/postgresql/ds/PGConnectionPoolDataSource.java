/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ds;

import org.postgresql.PGProperty;

import javax.naming.Reference;

import java.sql.DriverManager;
import java.util.Properties;

/**
 * DataSource for {@link software.aws.rds.jdbc.postgresql.Driver} similar
 * to {@link org.postgresql.ds.PGConnectionPoolDataSource}
 */
public class PGConnectionPoolDataSource extends org.postgresql.ds.PGConnectionPoolDataSource {

  /*
   * Ensure the driver is loaded as JDBC Driver might be invisible to Java's ServiceLoader.
   * Usually, {@code Class.forName(...)} is not required as {@link DriverManager} detects JDBC drivers
   * via {@code META-INF/services/java.sql.Driver} entries. However, there might be cases when the driver
   * is located at the application level classloader, thus it might be required to perform manual
   * registration of the driver.
   */
  static {
    try {
      Class.forName("software.aws.rds.jdbc.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
          "PGConnectionPoolDataSource is unable to load software.aws.rds.jdbc.postgresql.Driver. Please check if you have proper AWS PostgreSQL JDBC Driver jar on the classpath",
          e);
    }
  }

  /**
   * @return {@link DriverManager} supported driver protocol
   */
  @Override
  protected String getDriverProtocol() { return "jdbc:postgresql:aws://"; }

  /**
   * Sets properties from a {@link DriverManager} URL.
   *
   * @param url properties to set
   */
  @Override
  public void setUrl(String url) {

    Properties p = software.aws.rds.jdbc.postgresql.Driver.parseURL(url, null);

    if (p == null) {
      throw new IllegalArgumentException("URL invalid " + url);
    }
    for (PGProperty property : PGProperty.values()) {
      if (!this.properties.containsKey(property.getName())) {
        setProperty(property, property.get(p));
      }
    }
  }

  /**
   * Generates a reference using the appropriate object factory.
   *
   * @return reference using the appropriate object factory
   */
  @Override
  protected Reference createReference() {
    return new Reference(getClass().getName(), software.aws.rds.jdbc.postgresql.ds.common.PGObjectFactory.class.getName(), null);
  }

}
