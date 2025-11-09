package be.julienpiron.redis;

import java.time.Clock;
import java.time.Instant;
import java.util.LinkedList;
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

record StreamEntry(LinkedList<Stream> values) implements Entry {
  public StreamEntry() {
    this(new LinkedList<>());
  }

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

  public void setStream(String key, Stream stream) {
    map.putIfAbsent(key, new StreamEntry());

    Entry entry = map.get(key);

    if (!(entry instanceof StreamEntry streamEntry))
      throw new IllegalArgumentException(key + " is not a stream");

    if (!streamEntry.values().isEmpty()) {
      Stream lastStream = streamEntry.values().getLast();
      if ((stream.milliseconds() < lastStream.milliseconds())
          || (stream.milliseconds() == lastStream.milliseconds()
              && stream.sequence() <= lastStream.sequence())) {
        throw new IllegalArgumentException(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item");
      }
    }

    streamEntry.values().add(stream);
  }

  public String type(String key) {
    Entry entry = map.get(key);

    if (entry == null) return "none";

    return entry.type();
  }
}
