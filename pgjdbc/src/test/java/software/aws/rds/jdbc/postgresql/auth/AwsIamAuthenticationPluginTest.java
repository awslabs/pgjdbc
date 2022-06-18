package software.aws.rds.jdbc.postgresql.auth;


import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

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

  @Test
  public void testGetPasswordWithWrongType() throws PSQLException {
    Properties properties = new Properties();
    properties.setProperty(PGProperty.USER.getName(), "testUser");
    properties.setProperty(PGProperty.PG_HOST.getName(), "test.user.us-east-2.rds.amazonaws.com");
    AwsIamAuthenticationPlugin plugin = new AwsIamAuthenticationPlugin(properties);
    assertThrows(PSQLException.class, () -> plugin.getPassword(AuthenticationRequestType.MD5_PASSWORD));
  }

  @Test
  public void testGetPasswordValidTokenInCache() throws PSQLException, NoSuchFieldException,
      IllegalAccessException {
    Properties properties = new Properties();
    properties.setProperty(PGProperty.USER.getName(), "testUser");
    properties.setProperty(PGProperty.PG_HOST.getName(), "test.user.us-east-2.rds.amazonaws.com");
    AwsIamAuthenticationPlugin plugin = new AwsIamAuthenticationPlugin(properties);
    plugin.getClass().getDeclaredField("tokenCache").setAccessible(true);
    Field tokenField = plugin.getClass().getDeclaredField("tokenCache");
    tokenField.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(tokenField, tokenField.getModifiers() & ~Modifier.FINAL);
    ConcurrentHashMap<String, AwsIamAuthenticationPlugin.TokenInfo> value = new ConcurrentHashMap<>();
    value.clear();
    value.put("us-east-2:test.user.us-east-2.rds.amazonaws.com:5342:testUser", new AwsIamAuthenticationPlugin.TokenInfo("testToken", Instant.now().plusMillis(300000)));
    tokenField.set(plugin, value);

    char[] actualResult = plugin.getPassword(AuthenticationRequestType.CLEARTEXT_PASSWORD);
    assertArrayEquals("testToken".toCharArray(), actualResult);
  }

  @Test
  public void testGetPasswordExpiredTokenInCache() throws PSQLException, NoSuchFieldException,
      IllegalAccessException {
    Properties properties = new Properties();
    properties.setProperty(PGProperty.USER.getName(), "testUser");
    properties.setProperty(PGProperty.PG_HOST.getName(), "test.user.us-east-2.rds.amazonaws.com");
    AwsIamAuthenticationPlugin plugin = new AwsIamAuthenticationPlugin(properties);
    plugin.getClass().getDeclaredField("tokenCache").setAccessible(true);
    Field tokenField = plugin.getClass().getDeclaredField("tokenCache");
    tokenField.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(tokenField, tokenField.getModifiers() & ~Modifier.FINAL);
    ConcurrentHashMap<String, AwsIamAuthenticationPlugin.TokenInfo> value = new ConcurrentHashMap<>();
    value.clear();
    value.put("us-east-2:test.user.us-east-2.rds.amazonaws.com:5342:testUser", new AwsIamAuthenticationPlugin.TokenInfo("testToken", Instant.now().minusMillis(300000)));
    tokenField.set(plugin, value);

    AwsIamAuthenticationPlugin spyPlugin = Mockito.spy(plugin);
    when(spyPlugin.generateAuthenticationToken("testUser", "test.user.us-east-2.rds.amazonaws.com", 5342, Region.US_EAST_2)).thenReturn("generatedToken");
    char[] actualResult = spyPlugin.getPassword(AuthenticationRequestType.CLEARTEXT_PASSWORD);
    assertArrayEquals("generatedToken".toCharArray(), actualResult);
  }

  @Test
  public void testGetPasswordGenerateToken() throws PSQLException, NoSuchFieldException,
      IllegalAccessException {
    Properties properties = new Properties();
    properties.setProperty(PGProperty.USER.getName(), "testUser");
    properties.setProperty(PGProperty.PG_HOST.getName(), "test.user.us-east-2.rds.amazonaws.com");

    AwsIamAuthenticationPlugin plugin = new AwsIamAuthenticationPlugin(properties);
    plugin.getClass().getDeclaredField("tokenCache").setAccessible(true);
    Field tokenField = plugin.getClass().getDeclaredField("tokenCache");
    tokenField.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(tokenField, tokenField.getModifiers() & ~Modifier.FINAL);
    ConcurrentHashMap<String, AwsIamAuthenticationPlugin.TokenInfo> value = new ConcurrentHashMap<>();
    value.clear();
    tokenField.set(plugin, value);

    AwsIamAuthenticationPlugin spyPlugin = Mockito.spy(new AwsIamAuthenticationPlugin(properties));
    when(spyPlugin.generateAuthenticationToken("testUser", "test.user.us-east-2.rds.amazonaws.com", 5342, Region.US_EAST_2)).thenReturn("generatedToken");
    char[] actualResult = spyPlugin.getPassword(AuthenticationRequestType.CLEARTEXT_PASSWORD);
    assertArrayEquals("generatedToken".toCharArray(), actualResult);
  }

}


