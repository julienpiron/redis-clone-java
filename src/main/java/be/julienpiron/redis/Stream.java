package be.julienpiron.redis;

import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record Stream(Stream.ID id, List<String> values) {
  private static Pattern pattern =
      Pattern.compile("(?<milliseconds>[0-9]+)(-(?<sequence>[0-9]+|\\*))?");

  public record ID(long milliseconds, long sequence, Optional<ID> previous)
      implements Comparable<ID> {
    public static ID parse(String input, Optional<ID> previous, Clock clock) {
      if (input.equals("*")) {
        long milliseconds = clock.instant().toEpochMilli();
        return new ID(milliseconds, nextSequence(milliseconds, previous), previous);
      }

      Matcher matcher = pattern.matcher(input);

      if (!(matcher.matches())) {
        throw new IllegalArgumentException("ERR Invalid stream ID");
      }

      long milliseconds = Long.parseLong(matcher.group("milliseconds"));

      if (matcher.group("sequence") == null || matcher.group("sequence").equals("*")) {
        return new ID(milliseconds, nextSequence(milliseconds, previous), previous);
      }

      long sequence = Long.parseLong(matcher.group("sequence"));

      return new ID(milliseconds, sequence, previous);
    }

    public static ID parse(String input, Clock clock) {
      return parse(input, Optional.empty(), clock);
    }

    public ID {
      if (milliseconds == 0 && sequence == 0)
        throw new IllegalArgumentException("ERR The ID specified in XADD must be greater than 0-0");

      if (previous.isPresent() && milliseconds < previous.get().milliseconds)
        throw new IllegalArgumentException(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item");

      if (previous.isPresent()
          && milliseconds == previous.get().milliseconds
          && sequence <= previous.get().sequence)
        throw new IllegalArgumentException(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item");
    }

    private static long nextSequence(long milliseconds, Optional<Stream.ID> lastID) {

      if (lastID.isPresent() && milliseconds == lastID.get().milliseconds()) {
        return lastID.get().sequence() + 1;
      }

      return milliseconds == 0 ? 1 : 0;
    }

    @Override
    public String toString() {
      return milliseconds + "-" + sequence;
    }

    @Override
    public int compareTo(ID other) {
      int millisecondComparison = Long.compare(this.milliseconds, other.milliseconds);

      if (millisecondComparison == 0) {
        return Long.compare(this.sequence, other.sequence);
      }

      return millisecondComparison;
    }
  }
}
