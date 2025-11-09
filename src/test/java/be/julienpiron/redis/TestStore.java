package be.julienpiron.redis;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

public class TestStore extends Store {
  TestStore() {
    super(Clock.fixed(Instant.parse("1996-12-31T12:00:00.00Z"), ZoneId.systemDefault()));
  }

  public void advanceClock(Duration duration) {
    this.clock = Clock.offset(clock, duration);
  }
}
