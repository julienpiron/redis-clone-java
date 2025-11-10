package be.julienpiron.redis;

import java.time.Clock;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamEntry implements StoreEntry {
  private LinkedList<Stream> values;

  public StreamEntry() {
    this(new LinkedList<>());
  }

  public StreamEntry(LinkedList<Stream> values) {
    this.values = values;
  }

  @Override
  public String type() {
    return "stream";
  }

  public Stream.ID add(String stringId, List<String> data, Clock clock) {
    Stream.ID id = parseID(stringId, clock);

    validateID(id);

    values.add(new Stream(id, data));

    return id;
  }

  public List<Stream> getValues() {
    return values;
  }

  public void setValues(List<Stream> values) {
    this.values = new LinkedList<>(values);
  }

  private Optional<Stream.ID> getLastID() {
    try {
      return Optional.of(values.getLast().id());
    } catch (NoSuchElementException _) {
      return Optional.empty();
    }
  }

  public Stream.ID parseID(String input, Clock clock) {
    Pattern pattern = Pattern.compile("(?<milliseconds>[0-9]+)(-(?<sequence>[0-9]+|\\*))?");

    if (input.equals("*")) {
      long milliseconds = clock.instant().toEpochMilli();
      return new Stream.ID(milliseconds, nextSequence(milliseconds));
    }

    Matcher matcher = pattern.matcher(input);

    if (!(matcher.matches())) {
      throw new IllegalArgumentException("ERR Invalid stream ID");
    }

    long milliseconds = Long.parseLong(matcher.group("milliseconds"));

    if (matcher.group("sequence") == null || matcher.group("sequence").equals("*")) {
      return new Stream.ID(milliseconds, nextSequence(milliseconds));
    }

    long sequence = Long.parseLong(matcher.group("sequence"));

    return new Stream.ID(milliseconds, sequence);
  }

  public void validateID(Stream.ID id) {
    if (id.milliseconds() == 0 && id.sequence() == 0)
      throw new IllegalArgumentException("ERR The ID specified in XADD must be greater than 0-0");

    Optional<Stream.ID> lastID = getLastID();
    if (lastID.isPresent() && id.compareTo(lastID.get()) < 1)
      throw new IllegalArgumentException(
          "ERR The ID specified in XADD is equal or smaller than the target stream top item");
  }

  private long nextSequence(long milliseconds) {
    Optional<Stream.ID> lastID = getLastID();

    if (lastID.isPresent() && milliseconds == lastID.get().milliseconds()) {
      return lastID.get().sequence() + 1;
    }

    return milliseconds == 0 ? 1 : 0;
  }
}
