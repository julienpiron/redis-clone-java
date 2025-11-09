package be.julienpiron.redis;

import java.time.Clock;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

public class StreamEntry implements StoreEntry {
  private LinkedList<Stream> values;

  public StreamEntry() {
    this(new LinkedList<>());
  }

  public StreamEntry(LinkedList<Stream> values) {
    this.values = values;
  }

  @Override
  public String type() {
    return "stream";
  }

  public Stream.ID add(String id, List<String> data, Clock clock) {
    Stream.ID validID = Stream.ID.parse(id, getLastID(), clock);

    values.add(new Stream(validID, data));

    return validID;
  }

  public List<Stream> getValues() {
    return values;
  }

  public void setValues(List<Stream> values) {
    this.values = new LinkedList<>(values);
  }

  private Optional<Stream.ID> getLastID() {
    try {
      return Optional.of(values.getLast().id());
    } catch (NoSuchElementException _) {
      return Optional.empty();
    }
  }
}
