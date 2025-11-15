package be.julienpiron.redis;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record PartialStreamId(Optional<Long> milliseconds, Optional<Long> sequence) {

  public static PartialStreamId parse(String input) {
    if (input.equalsIgnoreCase("-")) {
      return new PartialStreamId(Optional.of(0L), Optional.of(0L));
    }

    if (input.equalsIgnoreCase("+")) {
      return new PartialStreamId(Optional.of(Long.MAX_VALUE), Optional.of(Long.MAX_VALUE));
    }

    Pattern pattern = Pattern.compile("(\\*|((?<milliseconds>\\d+)(-(?<sequence>\\d+))?))");
    Matcher matcher = pattern.matcher(input);

    if (!matcher.matches()) {
      throw new IllegalArgumentException("Impossible to parse this id");
    }

    String milliseconds = matcher.group("milliseconds");
    String sequence = matcher.group("sequence");

    return new PartialStreamId(
        milliseconds != null ? Optional.of(Long.parseLong(milliseconds)) : Optional.empty(),
        sequence != null ? Optional.of(Long.parseLong(sequence)) : Optional.empty());
  }

  public StreamId from() {
    return new StreamId(milliseconds().orElse(0L), sequence().orElse(0L));
  }

  public StreamId to() {
    return new StreamId(milliseconds().orElse(Long.MAX_VALUE), sequence().orElse(Long.MAX_VALUE));
  }
}
