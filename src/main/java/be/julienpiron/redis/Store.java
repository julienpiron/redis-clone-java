package be.julienpiron.redis;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Store {
  private final ConcurrentHashMap<String, StoreEntry> map;
  private final Logger logger = LoggerFactory.getLogger(Store.class);
  protected volatile Clock clock;

  public Store() {
    this(Clock.systemDefaultZone());
  }

  protected Store(Clock clock) {
    map = new ConcurrentHashMap<>();
    this.clock = clock;
  }

  public String getString(String key) {
    logger.debug(clock.instant().toString());
    logger.debug("Getting: {}", key);

    StoreEntry entry = map.get(key);

    logger.debug("Got: " + entry);

    if (!(entry instanceof StringEntry stringEntry)) {
      return null;
    }

    if (stringEntry.isExpired(clock)) {
      logger.debug("Entry " + key + " is expired");
      map.remove(key);
      return null;
    }

    return stringEntry.value();
  }

  public void setString(String key, String value) {
    map.put(key, new StringEntry(value, Optional.empty()));
  }

  public void setString(String key, String value, Duration expiry) {
    map.put(key, new StringEntry(value, Optional.of(clock.instant().plus(expiry))));
  }

  public StreamEntry getStream(String key) {
    logger.debug("Getting: {}", key);

    StoreEntry entry = map.get(key);

    logger.debug("Got: " + entry);

    if (!(entry instanceof StreamEntry streamEntry))
      throw new IllegalArgumentException(key + " is not a stream");

    return streamEntry;
  }

  public String setStream(String key, String id, List<String> values) {
    map.putIfAbsent(key, new StreamEntry());

    StoreEntry entry = map.get(key);

    if (!(entry instanceof StreamEntry streamEntry))
      throw new IllegalArgumentException(key + " is not a stream");

    String generatedID = streamEntry.add(id, values, clock).toString();

    return generatedID;
  }

  public String type(String key) {
    StoreEntry entry = map.get(key);

    if (entry == null) return "none";

    return entry.type();
  }
}
