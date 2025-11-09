package be.julienpiron.redis;

import java.time.Clock;
import java.util.Map;

public record Stream(Stream.ID id, Map<String, String> values) {
  public record ID(long milliseconds, long sequence) {
    public ID {
      if (milliseconds == 0 && sequence == 0)
        throw new IllegalArgumentException("ERR The ID specified in XADD must be greater than 0-0");
    }

    public ID(long milliseconds, long sequence, ID previous) {
      if (milliseconds < previous.milliseconds)
        throw new IllegalArgumentException(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item");

      if (milliseconds == previous.milliseconds && sequence <= previous.sequence)
        throw new IllegalArgumentException(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item");

      this(milliseconds, sequence);
    }

    public ID(Clock clock) {
      this(clock.instant().toEpochMilli(), 0);
    }

    public ID(Clock clock, ID previous) {
      long currentEpoch = clock.instant().toEpochMilli();
      this(currentEpoch, (previous.milliseconds == currentEpoch) ? previous.sequence + 1 : 0);
    }

    public ID(long milliseconds) {
      this(milliseconds, milliseconds == 0 ? 1 : 0);
    }

    public ID(long milliseconds, ID previous) {
      if (milliseconds < previous.milliseconds)
        throw new IllegalArgumentException(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item");

      this(milliseconds, (previous.milliseconds == milliseconds) ? previous.sequence + 1 : 0);
    }

    @Override
    public String toString() {
      return milliseconds + "-" + sequence;
    }
  }
}
