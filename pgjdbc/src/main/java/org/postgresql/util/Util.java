/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.util;

import org.checkerframework.checker.nullness.qual.Nullable;

public class Util {

  private static @Nullable String shadingPrefix = null;
  private static final Object lockObj = new Object();

  /**
   * Get the name of the package that the supplied class belongs to
   *
   * @param clazz the {@link Class} to analyze
   * @return the name of the package that the supplied class belongs to
   */
  public static String getPackageName(Class<?> clazz) {
    String fqcn = clazz.getName();
    int classNameStartsAt = fqcn.lastIndexOf('.');
    if (classNameStartsAt > 0) {
      return fqcn.substring(0, classNameStartsAt);
    }
    return "";
  }

  /**
   * Adds the shading package prefix to a class name and returns it.
   *
   * @param clazzName Class name.
   *
   * @return the shading prefix "software.aws.rds.jdbc.shading.{clazzName}" or just {clazzName}
   */
  public static String shadingPrefix(String clazzName) {
    if (shadingPrefix == null) {
      // lazy init
      synchronized (lockObj) {
        if (shadingPrefix == null) {
          shadingPrefix = getPackageName(Util.class).replaceAll("org.postgresql.util", "");
        }
      }
    }
    if ("".equals(shadingPrefix)) {
      return clazzName;
    }
    return shadingPrefix + clazzName;
  }
}
