package be.julienpiron.redis;

import java.util.List;

public record Stream(Stream.ID id, List<String> values) {
  public record ID(long milliseconds, long sequence) implements Comparable<ID> {
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
