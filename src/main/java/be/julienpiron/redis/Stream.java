package be.julienpiron.redis;

import java.util.Map;

public record Stream(long milliseconds, long sequence, Map<String, String> values) {
  public Stream {
    if (milliseconds == 0 && sequence == 0)
      throw new IllegalArgumentException("ERR The ID specified in XADD must be greater than 0-0");
  }

  public String id() {
    return milliseconds + "-" + sequence;
  }
}
