package be.julienpiron.redis;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

record StringEntry(String value, Optional<Instant> expiry) {
  boolean isExpired(Clock clock) {
    if (expiry.isEmpty()) {
      return false;
    }

    return Instant.now(clock).isAfter(expiry.get());
  }

  String type() {
    return "string";
  }
}

public class Store {
  private final ConcurrentHashMap<String, StringEntry> map;
  private final Logger logger = LoggerFactory.getLogger(Store.class);

  public Store() {
    map = new ConcurrentHashMap<>();
  }

  public String get(String key, Clock clock) {
    logger.debug("Getting: " + key);

    StringEntry entry = map.get(key);

    logger.debug("Got: " + entry);

    if (entry == null) {
      return null;
    }

    if (entry.isExpired(clock)) {
      logger.debug("Entry " + key + " is expired");
      map.remove(key);
      return null;
    }

    return entry.value();
  }

  public void set(String key, String value) {
    map.put(key, new StringEntry(value, Optional.empty()));
  }

  public void set(String key, String value, Instant expiry) {
    map.put(key, new StringEntry(value, Optional.of(expiry)));
  }

  public String type(String key) {
    StringEntry entry = map.get(key);

    if (entry == null) return "none";

    return entry.type();
  }
}
