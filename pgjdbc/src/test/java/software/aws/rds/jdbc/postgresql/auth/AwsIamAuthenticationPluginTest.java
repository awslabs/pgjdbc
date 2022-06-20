package software.aws.rds.jdbc.postgresql.auth;


import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;

import org.postgresql.PGProperty;
import org.postgresql.plugin.AuthenticationRequestType;
import org.postgresql.util.PSQLException;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.regions.Region;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class AwsIamAuthenticationPluginTest {

  private Properties properties;
  private AwsIamAuthenticationPlugin plugin;
  private static final String TEST_USER = "testUser";
  private static final String TEST_HOST = "test.user.us-east-2.rds.amazonaws.com";
  private static final String TEST_TOKEN = "testToken";
  private static final String TOKEN_FIELD = "tokenCache";
  private static final String GENERATED_TOKEN = "generatedToken";
  private static final String CACHE_KEY = "us-east-2:test.user.us-east-2.rds.amazonaws.com:5342:testUser";

  @BeforeEach
  public void init() throws PSQLException {
    properties = new Properties();
    properties.setProperty(PGProperty.USER.getName(), TEST_USER);
    properties.setProperty(PGProperty.PG_HOST.getName(), TEST_HOST);
    plugin = new AwsIamAuthenticationPlugin(properties);
  }

  @Test
  public void testGetPasswordWithWrongType() throws PSQLException {
    assertThrows(PSQLException.class, () -> plugin.getPassword(AuthenticationRequestType.MD5_PASSWORD));
  }

  @Test
  public void testGetPasswordValidTokenInCache() throws PSQLException, NoSuchFieldException,
      IllegalAccessException {
    plugin.getClass().getDeclaredField(TOKEN_FIELD).setAccessible(true);
    Field tokenField = plugin.getClass().getDeclaredField(TOKEN_FIELD);
    tokenField.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(tokenField, tokenField.getModifiers() & ~Modifier.FINAL);
    ConcurrentHashMap<String, AwsIamAuthenticationPlugin.TokenInfo> value = new ConcurrentHashMap<>();
    value.clear();
    value.put(CACHE_KEY, new AwsIamAuthenticationPlugin.TokenInfo(TEST_TOKEN, Instant.now().plusMillis(300000)));
    tokenField.set(plugin, value);

    char[] actualResult = plugin.getPassword(AuthenticationRequestType.CLEARTEXT_PASSWORD);
    assertArrayEquals(TEST_TOKEN.toCharArray(), actualResult);
  }

  @Test
  public void testGetPasswordExpiredTokenInCache() throws PSQLException, NoSuchFieldException,
      IllegalAccessException {
    plugin.getClass().getDeclaredField(TOKEN_FIELD).setAccessible(true);
    Field tokenField = plugin.getClass().getDeclaredField(TOKEN_FIELD);
    tokenField.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(tokenField, tokenField.getModifiers() & ~Modifier.FINAL);
    ConcurrentHashMap<String, AwsIamAuthenticationPlugin.TokenInfo> value = new ConcurrentHashMap<>();
    value.clear();
    value.put(CACHE_KEY, new AwsIamAuthenticationPlugin.TokenInfo(TEST_TOKEN, Instant.now().minusMillis(300000)));
    tokenField.set(plugin, value);

    AwsIamAuthenticationPlugin spyPlugin = Mockito.spy(plugin);
    when(spyPlugin.generateAuthenticationToken(TEST_USER, TEST_HOST, 5342, Region.US_EAST_2)).thenReturn(GENERATED_TOKEN);
    char[] actualResult = spyPlugin.getPassword(AuthenticationRequestType.CLEARTEXT_PASSWORD);
    assertArrayEquals(GENERATED_TOKEN.toCharArray(), actualResult);
  }

  @Test
  public void testGetPasswordGenerateToken() throws PSQLException, NoSuchFieldException,
      IllegalAccessException {
    plugin.getClass().getDeclaredField(TOKEN_FIELD).setAccessible(true);
    Field tokenField = plugin.getClass().getDeclaredField(TOKEN_FIELD);
    tokenField.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(tokenField, tokenField.getModifiers() & ~Modifier.FINAL);
    ConcurrentHashMap<String, AwsIamAuthenticationPlugin.TokenInfo> value = new ConcurrentHashMap<>();
    value.clear();
    tokenField.set(plugin, value);

    AwsIamAuthenticationPlugin spyPlugin = Mockito.spy(new AwsIamAuthenticationPlugin(properties));
    when(spyPlugin.generateAuthenticationToken(TEST_USER, TEST_HOST, 5342, Region.US_EAST_2)).thenReturn(GENERATED_TOKEN);
    char[] actualResult = spyPlugin.getPassword(AuthenticationRequestType.CLEARTEXT_PASSWORD);
    assertArrayEquals(GENERATED_TOKEN.toCharArray(), actualResult);
  }

}


