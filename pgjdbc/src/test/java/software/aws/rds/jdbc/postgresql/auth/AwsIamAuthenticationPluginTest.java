/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.auth;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import org.postgresql.PGProperty;
import org.postgresql.plugin.AuthenticationRequestType;
import org.postgresql.util.PSQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;

import software.amazon.awssdk.regions.Region;

import java.time.Instant;
import java.util.Properties;

public class AwsIamAuthenticationPluginTest {

  private Properties properties;
  private AwsIamAuthenticationPlugin targetPlugin;
  private static final String TEST_USER = "testUser";
  private static final String TEST_HOST = "test.user.us-east-2.rds.amazonaws.com";
  private static final String TEST_TOKEN = "testToken";
  private static final String GENERATED_TOKEN = "generatedToken";
  private static final String CACHE_KEY = "us-east-2:test.user.us-east-2.rds.amazonaws.com:5342:testUser";

  @BeforeEach
  public void initTargetPlugin() throws PSQLException {
    properties = new Properties();
    properties.setProperty(PGProperty.USER.getName(), TEST_USER);
    properties.setProperty(PGProperty.PG_HOST.getName(), TEST_HOST);
    targetPlugin = new AwsIamAuthenticationPlugin(properties);
    AwsIamAuthenticationPlugin.tokenCache.clear();
  }

  @Test
  public void testGetPasswordWithWrongType() throws PSQLException {
    assertThrows(PSQLException.class, () -> targetPlugin.getPassword(AuthenticationRequestType.MD5_PASSWORD));
  }

  @Test
  public void testGetPasswordValidTokenInCache() throws PSQLException {
    AwsIamAuthenticationPlugin.tokenCache.put(CACHE_KEY, new AwsIamAuthenticationPlugin.TokenInfo(TEST_TOKEN, Instant.now().plusMillis(300000)));
    char[] actualResult = targetPlugin.getPassword(AuthenticationRequestType.CLEARTEXT_PASSWORD);

    assertArrayEquals(TEST_TOKEN.toCharArray(), actualResult);
  }

  @Test
  public void testGetPasswordExpiredTokenInCache() throws PSQLException {
    AwsIamAuthenticationPlugin.tokenCache.put(CACHE_KEY, new AwsIamAuthenticationPlugin.TokenInfo(TEST_TOKEN, Instant.now().minusMillis(300000)));
    AwsIamAuthenticationPlugin spyPlugin = Mockito.spy(targetPlugin);
    when(spyPlugin.generateAuthenticationToken(TEST_USER, TEST_HOST, 5342, Region.US_EAST_2)).thenReturn(GENERATED_TOKEN);
    char[] actualResult = spyPlugin.getPassword(AuthenticationRequestType.CLEARTEXT_PASSWORD);

    assertArrayEquals(GENERATED_TOKEN.toCharArray(), actualResult);
    assertEquals(GENERATED_TOKEN, AwsIamAuthenticationPlugin.tokenCache.get(CACHE_KEY).getToken());
  }

  @Test
  public void testGetPasswordEmptyCache() throws PSQLException {
    AwsIamAuthenticationPlugin spyPlugin = Mockito.spy(new AwsIamAuthenticationPlugin(properties));
    when(spyPlugin.generateAuthenticationToken(TEST_USER, TEST_HOST, 5342, Region.US_EAST_2)).thenReturn(GENERATED_TOKEN);
    char[] actualResult = spyPlugin.getPassword(AuthenticationRequestType.CLEARTEXT_PASSWORD);

    assertArrayEquals(GENERATED_TOKEN.toCharArray(), actualResult);
    assertEquals(GENERATED_TOKEN, AwsIamAuthenticationPlugin.tokenCache.get(CACHE_KEY).getToken());
  }
}
