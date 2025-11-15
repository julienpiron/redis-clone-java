package be.julienpiron.redis;

public record StreamId(long milliseconds, long sequence) implements Comparable<StreamId> {
  @Override
  public String toString() {
    return milliseconds + "-" + sequence;
  }

  @Override
  public int compareTo(StreamId other) {
    int millisecondComparison = Long.compare(this.milliseconds, other.milliseconds);

    if (millisecondComparison == 0) {
      return Long.compare(this.sequence, other.sequence);
    }

    return millisecondComparison;
  }
}
