package be.julienpiron.redis;

import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface Entry {
  String type();
}

record StringEntry(String value, Optional<Instant> expiry) implements Entry {
  boolean isExpired(Clock clock) {
    if (expiry.isEmpty()) {
      return false;
    }

    return Instant.now(clock).isAfter(expiry.get());
  }

  @Override
  public String type() {
    return "string";
  }
}

record StreamEntry(LinkedHashMap<String, Map<String, String>> value) implements Entry {

  @Override
  public String type() {
    return "stream";
  }
}

public class Store {
  private final ConcurrentHashMap<String, Entry> map;
  private final Logger logger = LoggerFactory.getLogger(Store.class);

  public Store() {
    map = new ConcurrentHashMap<>();
  }

  public String getString(String key, Clock clock) {
    logger.debug("Getting: " + key);

    Entry entry = map.get(key);

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

  public void setString(String key, String value, Instant expiry) {
    map.put(key, new StringEntry(value, Optional.of(expiry)));
  }

  public void setStream(String key, LinkedHashMap<String, Map<String, String>> stream) {
    map.put(key, new StreamEntry(stream));
  }

  public String type(String key) {
    Entry entry = map.get(key);

    if (entry == null) return "none";

    return entry.type();
  }
}
