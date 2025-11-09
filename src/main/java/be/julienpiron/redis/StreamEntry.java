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
    Optional<Stream> last = getLast();

    if (input.equals("*")) {
      return last.isPresent() ? new Stream.ID(clock, last.get().id()) : new Stream.ID(clock);
    }

    Pattern pattern = Pattern.compile("(?<milliseconds>[0-9]+)-(?<sequence>[0-9]+|\\*)");
    Matcher matcher = pattern.matcher(input);

    if (!(matcher.matches())) {
      throw new IllegalArgumentException("ERR Invalid stream ID");
    }

    long milliseconds = Long.parseLong(matcher.group("milliseconds"));

    if (matcher.group("sequence").equals("*")) {
      return last.isPresent()
          ? new Stream.ID(milliseconds, last.get().id())
          : new Stream.ID(milliseconds);
    }

    long sequence = Long.parseLong(matcher.group("sequence"));

    return last.isPresent()
        ? new Stream.ID(milliseconds, sequence, last.get().id())
        : new Stream.ID(milliseconds, sequence);
  }

  private Optional<Stream> getLast() {
    try {
      return Optional.of(values.getLast());
    } catch (NoSuchElementException _) {
      return Optional.empty();
    }
  }
}
