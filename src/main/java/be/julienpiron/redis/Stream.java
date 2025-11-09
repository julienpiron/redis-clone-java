package be.julienpiron.redis;

import java.util.Map;
import java.util.Optional;

public record Stream(Stream.ID id, Map<String, String> values) {
  public record ID(long milliseconds, long sequence, Optional<ID> previous) {
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

    @Override
    public String toString() {
      return milliseconds + "-" + sequence;
    }
  }
}
