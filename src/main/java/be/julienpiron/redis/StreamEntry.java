package be.julienpiron.redis;

import java.time.Clock;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record StreamEntry(LinkedList<Stream> values) implements StoreEntry {
  public StreamEntry() {
    this(new LinkedList<>());
  }

  @Override
  public String type() {
    return "stream";
  }

  public Stream.ID add(String id, Map<String, String> data, Clock clock) {
    Stream.ID validID = validateID(id, clock);

    values.add(new Stream(validID, data));

    return validID;
  }

  private Stream.ID validateID(String input, Clock clock) {
    if (input.equals("*")) {
      long milliseconds = clock.instant().toEpochMilli();
      return new Stream.ID(milliseconds, nextSequence(milliseconds), getLastID());
    }

    Pattern pattern = Pattern.compile("(?<milliseconds>[0-9]+)-(?<sequence>[0-9]+|\\*)");
    Matcher matcher = pattern.matcher(input);

    if (!(matcher.matches())) {
      throw new IllegalArgumentException("ERR Invalid stream ID");
    }

    long milliseconds = Long.parseLong(matcher.group("milliseconds"));

    if (matcher.group("sequence").equals("*")) {
      return new Stream.ID(milliseconds, nextSequence(milliseconds), getLastID());
    }

    long sequence = Long.parseLong(matcher.group("sequence"));

    return new Stream.ID(milliseconds, sequence, getLastID());
  }

  private Optional<Stream.ID> getLastID() {
    try {
      return Optional.of(values.getLast().id());
    } catch (NoSuchElementException _) {
      return Optional.empty();
    }
  }

  private long nextSequence(long milliseconds) {
    Optional<Stream.ID> lastID = getLastID();

    if (lastID.isPresent() && milliseconds == lastID.get().milliseconds()) {
      return lastID.get().sequence() + 1;
    }

    return milliseconds == 0 ? 1 : 0;
  }
}
