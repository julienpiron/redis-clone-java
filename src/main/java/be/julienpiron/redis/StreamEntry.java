package be.julienpiron.redis;

import java.time.Clock;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamEntry implements StoreEntry {
  private TreeMap<StreamId, Stream> streams = new TreeMap<>();

  @Override
  public String type() {
    return "stream";
  }

  public StreamId add(String stringId, List<String> data, Clock clock) {
    StreamId id = parseID(stringId, clock);

    validateID(id);

    streams.put(id, new Stream(data));

    return id;
  }

  public TreeMap<StreamId, Stream> getValues() {
    return streams;
  }

  private Optional<StreamId> getLastID() {
    try {
      return Optional.of(streams.lastKey());
    } catch (NoSuchElementException _) {
      return Optional.empty();
    }
  }

  public StreamId parseID(String input, Clock clock) {
    Pattern pattern = Pattern.compile("(?<milliseconds>[0-9]+)(-(?<sequence>[0-9]+|\\*))?");

    if (input.equals("*")) {
      long milliseconds = clock.instant().toEpochMilli();
      return new StreamId(milliseconds, nextSequence(milliseconds));
    }

    Matcher matcher = pattern.matcher(input);

    if (!(matcher.matches())) {
      throw new IllegalArgumentException("ERR Invalid stream ID");
    }

    long milliseconds = Long.parseLong(matcher.group("milliseconds"));

    if (matcher.group("sequence") == null || matcher.group("sequence").equals("*")) {
      return new StreamId(milliseconds, nextSequence(milliseconds));
    }

    long sequence = Long.parseLong(matcher.group("sequence"));

    return new StreamId(milliseconds, sequence);
  }

  public void validateID(StreamId id) {
    if (id.milliseconds() == 0 && id.sequence() == 0)
      throw new IllegalArgumentException("ERR The ID specified in XADD must be greater than 0-0");

    Optional<StreamId> lastID = getLastID();
    if (lastID.isPresent() && id.compareTo(lastID.get()) < 1)
      throw new IllegalArgumentException(
          "ERR The ID specified in XADD is equal or smaller than the target stream top item");
  }

  private long nextSequence(long milliseconds) {
    Optional<StreamId> lastID = getLastID();

    if (lastID.isPresent() && milliseconds == lastID.get().milliseconds()) {
      return lastID.get().sequence() + 1;
    }

    return milliseconds == 0 ? 1 : 0;
  }
}
