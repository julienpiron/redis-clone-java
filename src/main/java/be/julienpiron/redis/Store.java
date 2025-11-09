package be.julienpiron.redis;

import java.time.Clock;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

  public long generateSequence(long milliseconds) {
    try {
      Stream last = values.getLast();

      if (milliseconds == last.milliseconds()) {
        return last.sequence() + 1;
      }

      return 0;
    } catch (NoSuchElementException _) {
      if (milliseconds == 0) {
        return 1;
      }
      return 0;
    }
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

  public String setStream(String key, String id, Map<String, String> values) {
    map.putIfAbsent(key, new StreamEntry());

    Entry entry = map.get(key);

    if (!(entry instanceof StreamEntry streamEntry))
      throw new IllegalArgumentException(key + " is not a stream");

    Pattern idPattern = Pattern.compile("(?<milliseconds>[0-9]+)-(?<sequence>[0-9]+|\\*)");
    Matcher matcher = idPattern.matcher(id);

    if (!matcher.matches()) {
      throw new IllegalArgumentException("ERR Invalid stream ID");
    }

    long milliseconds = Long.parseLong(matcher.group("milliseconds"));

    String sequenceGroup = matcher.group("sequence");
    long sequence =
        sequenceGroup.equals("*")
            ? streamEntry.generateSequence(milliseconds)
            : Long.parseLong(sequenceGroup);

    Stream newStream = new Stream(milliseconds, sequence, values);

    if (!streamEntry.values().isEmpty()) {
      Stream lastStream = streamEntry.values().getLast();
      if ((newStream.milliseconds() < lastStream.milliseconds())
          || (newStream.milliseconds() == lastStream.milliseconds()
              && newStream.sequence() <= lastStream.sequence())) {
        throw new IllegalArgumentException(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item");
      }
    }

    streamEntry.values().add(newStream);

    return newStream.id();
  }

  public String type(String key) {
    Entry entry = map.get(key);

    if (entry == null) return "none";

    return entry.type();
  }
}
