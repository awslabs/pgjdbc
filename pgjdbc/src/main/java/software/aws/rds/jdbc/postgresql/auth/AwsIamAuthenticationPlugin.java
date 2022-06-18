package software.aws.rds.jdbc.postgresql.auth;

import org.postgresql.PGProperty;
import org.postgresql.plugin.AuthenticationPlugin;
import org.postgresql.plugin.AuthenticationRequestType;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;

import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsUtilities;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AwsIamAuthenticationPlugin implements AuthenticationPlugin {

  private static final Logger LOGGER = Logger.getLogger(AwsIamAuthenticationPlugin.class.getName());
  private static final int REGION_MATCHER_GROUP = 3;
  private static final int DEFAULT_PORT = 5342;

  /**
   *  Default token expiration used by {@link RdsUtilities#generateAuthenticationToken}
   */
  private static final int DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60; // 15 min;

  private static final String PROPERTY_NAME_TOKEN_EXPIRATION = "iamTokenCacheExpiration";
  private static final ConcurrentHashMap<String, TokenInfo> tokenCache = new ConcurrentHashMap<>();

  private final Properties info;
  private final String user;
  private final String hostname;
  private final int port;
  private final Region region;
  private final String cacheKey;
  private final int tokenExpirationSec;

  public AwsIamAuthenticationPlugin(Properties info) throws PSQLException {
    this.info = info;
    this.user = PGProperty.USER.get(this.info);
    this.hostname = PGProperty.PG_HOST.get(this.info);

    final String portProperty = info.getProperty(PGProperty.PG_PORT.getName());
    this.port = (portProperty == null)
        ? DEFAULT_PORT
        : Integer.parseInt(portProperty);

    this.region = getRdsRegion(hostname);
    this.cacheKey = getCacheKey(user, hostname, port, region);

    final String expirationProperty = info.getProperty(PROPERTY_NAME_TOKEN_EXPIRATION);
    this.tokenExpirationSec = (expirationProperty == null)
        ? DEFAULT_TOKEN_EXPIRATION_SEC
        : Integer.parseInt(expirationProperty);
  }

  @Override
  public char @Nullable [] getPassword(AuthenticationRequestType type) throws PSQLException {

    if (type != AuthenticationRequestType.CLEARTEXT_PASSWORD) {
      final String exceptionMessage = "Authentication type CLEARTEXT_PASSWORD is expected.";
      LOGGER.log(Level.FINEST, exceptionMessage);
      throw new PSQLException(
          GT.tr(exceptionMessage),
          null);
    }

    TokenInfo tokenInfo = tokenCache.get(this.cacheKey);

    if (tokenInfo != null && !tokenInfo.isExpired()) {
      return tokenInfo.getToken().toCharArray();
    }

    final String token = generateAuthenticationToken(user, hostname, port, region);
    tokenCache.put(
        this.cacheKey,
        new TokenInfo(token, Instant.now().plus(this.tokenExpirationSec, ChronoUnit.SECONDS)));

    return token.toCharArray();
  }

  private String getCacheKey(
    final String user,
    final String hostname,
    final int port,
    final Region region) {

    return String.format("%s:%s:%d:%s", region, hostname, port, user);
  }

  protected String generateAuthenticationToken(
      final String user,
      final String hostname,
      final int port,
      final Region region) {

    RdsUtilities utilities = RdsUtilities.builder()
        .credentialsProvider(DefaultCredentialsProvider.create())
        .region(region)
        .build();

    return utilities.generateAuthenticationToken((builder) ->
        builder
            .hostname(hostname)
            .port(port)
            .username(user)
    );
  }

  private Region getRdsRegion(final String hostname) throws PSQLException {
    // Check Hostname
    final Pattern auroraDnsPattern =
        Pattern.compile(
            "(.+)\\.(proxy-|cluster-|cluster-ro-|cluster-custom-)?[a-zA-Z0-9]+\\.([a-zA-Z0-9\\-]+)\\.rds\\.amazonaws\\.com",
            Pattern.CASE_INSENSITIVE);
    final Matcher matcher = auroraDnsPattern.matcher(hostname);
    if (!matcher.find()) {
      // Does not match Amazon's Hostname, throw exception
      final String exceptionMessage = String.format("Unsupported AWS hostname '%s'. "
          + "Amazon domain name in format *.AWS-Region.rds.amazonaws.com is expected", hostname);

      LOGGER.log(Level.FINEST, exceptionMessage);
      throw new PSQLException(
          GT.tr(exceptionMessage),
          null);
    }

    // Get Region
    final String rdsRegion = matcher.group(REGION_MATCHER_GROUP);

    // Check Region
    Optional<Region> regionOptional = Region.regions().stream()
        .filter(r -> r.id().equalsIgnoreCase(rdsRegion))
        .findFirst();

    if (!regionOptional.isPresent()) {
      final String exceptionMessage = String.format("Unsupported AWS region '%s'. "
          + "For supported regions, please read "
          + "https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html\n",
          rdsRegion);

      LOGGER.log(Level.FINEST, exceptionMessage);
      throw new PSQLException(
          GT.tr(exceptionMessage),
          null);
    }
    return regionOptional.get();
  }

  public static class TokenInfo {

    private final String token;
    private final Instant expiration;

    public TokenInfo(String token, Instant expiration) {
      this.token = token;
      this.expiration = expiration;
    }

    public String getToken() { return this.token; }

    public Instant getExpiration() { return this.expiration; }

    public boolean isExpired() {
      return Instant.now().isAfter(this.expiration);
    }
  }
}
