/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ds.common;

import static org.postgresql.util.Util.shadingPrefix;

import org.postgresql.ds.PGPoolingDataSource;
import org.postgresql.ds.common.BaseDataSource;
import org.postgresql.util.internal.Nullness;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

/**
 * Returns a DataSource-ish thing based on a JNDI reference. In the case of a SimpleDataSource or
 * ConnectionPool, a new instance is created each time, as there is no connection state to maintain.
 * In the case of a PoolingDataSource, the same DataSource will be returned for every invocation
 * within the same VM/ClassLoader, so that the state of the connections in the pool will be
 * consistent.

 * The class is for {@link software.aws.rds.jdbc.postgresql.Driver} and is similar
 * to {@link org.postgresql.ds.common.PGObjectFactory}
 *
 */
public class PGObjectFactory implements ObjectFactory {
  /**
   * Dereferences a PostgreSQL DataSource. Other types of references are ignored.
   */
  public @Nullable Object getObjectInstance(Object obj, Name name, Context nameCtx,
      Hashtable<?, ?> environment) throws Exception {
    Reference ref = (Reference) obj;
    String className = ref.getClassName();
    // Old names are here for those who still use them
    if (className.equals(shadingPrefix("org.postgresql.ds.PGSimpleDataSource"))
        || className.equals("software.aws.rds.jdbc.postgresql.ds.PGSimpleDataSource")
        || className.equals(shadingPrefix("org.postgresql.jdbc2.optional.SimpleDataSource"))
        || className.equals(shadingPrefix("org.postgresql.jdbc3.Jdbc3SimpleDataSource"))) {
      return loadSimpleDataSource(ref);
    } else if (className.equals(shadingPrefix("org.postgresql.ds.PGConnectionPoolDataSource"))
        || className.equals("software.aws.rds.jdbc.postgresql.ds.PGConnectionPoolDataSource")
        || className.equals(shadingPrefix("org.postgresql.jdbc2.optional.ConnectionPool"))
        || className.equals(shadingPrefix("org.postgresql.jdbc3.Jdbc3ConnectionPool"))) {
      return loadConnectionPool(ref);
    } else if (className.equals(shadingPrefix("org.postgresql.ds.PGPoolingDataSource"))
        || className.equals("software.aws.rds.jdbc.postgresql.ds.PGPoolingDataSource")
        || className.equals(shadingPrefix("org.postgresql.jdbc2.optional.PoolingDataSource"))
        || className.equals(shadingPrefix("org.postgresql.jdbc3.Jdbc3PoolingDataSource"))) {
      return loadPoolingDataSource(ref);
    } else {
      return null;
    }
  }

  private Object loadPoolingDataSource(Reference ref) {
    // If DataSource exists, return it
    String name = Nullness.castNonNull(getProperty(ref, "dataSourceName"));
    PGPoolingDataSource pds = software.aws.rds.jdbc.postgresql.ds.PGPoolingDataSource.getDataSource(name);
    if (pds != null) {
      return pds;
    }
    // Otherwise, create a new one
    pds = new software.aws.rds.jdbc.postgresql.ds.PGPoolingDataSource();
    pds.setDataSourceName(name);
    loadBaseDataSource(pds, ref);
    String min = getProperty(ref, "initialConnections");
    if (min != null) {
      pds.setInitialConnections(Integer.parseInt(min));
    }
    String max = getProperty(ref, "maxConnections");
    if (max != null) {
      pds.setMaxConnections(Integer.parseInt(max));
    }
    return pds;
  }

  private Object loadSimpleDataSource(Reference ref) {
    software.aws.rds.jdbc.postgresql.ds.PGSimpleDataSource ds = new software.aws.rds.jdbc.postgresql.ds.PGSimpleDataSource();
    return loadBaseDataSource(ds, ref);
  }

  private Object loadConnectionPool(Reference ref) {
    software.aws.rds.jdbc.postgresql.ds.PGConnectionPoolDataSource cp = new software.aws.rds.jdbc.postgresql.ds.PGConnectionPoolDataSource();
    return loadBaseDataSource(cp, ref);
  }

  protected Object loadBaseDataSource(BaseDataSource ds, Reference ref) {
    ds.setFromReference(ref);

    return ds;
  }

  protected @Nullable String getProperty(Reference ref, String s) {
    RefAddr addr = ref.get(s);
    if (addr == null) {
      return null;
    }
    return (String) addr.getContent();
  }

}
