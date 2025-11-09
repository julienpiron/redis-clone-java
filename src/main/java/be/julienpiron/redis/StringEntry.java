package be.julienpiron.redis;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;

public record StringEntry(String value, Optional<Instant> expiry) implements StoreEntry {

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
