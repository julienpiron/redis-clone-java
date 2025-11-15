package be.julienpiron.redis;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KeyLock {
  private Lock lock = new ReentrantLock();
  private Condition condition = lock.newCondition();

  public void lock() {
    this.lock.lock();
  }

  public boolean tryLock(long nanos) throws InterruptedException {
    return lock.tryLock(nanos, TimeUnit.NANOSECONDS);
  }

  public void unlock() {
    this.lock.unlock();
  }

  public long awaitNanos(long nanosTimeout) throws InterruptedException {
    return condition.awaitNanos(nanosTimeout);
  }

  public void signalAll() {
    condition.signalAll();
  }
}

public class Store {
  private final ConcurrentHashMap<String, StoreEntry> map;
  private final ConcurrentHashMap<String, KeyLock> locks;
  private final Logger logger = LoggerFactory.getLogger(Store.class);
  protected volatile Clock clock;

  public Store() {
    this(Clock.systemDefaultZone());
  }

  protected Store(Clock clock) {
    map = new ConcurrentHashMap<>();
    locks = new ConcurrentHashMap<>();
    this.clock = clock;
  }

  protected KeyLock getKeyLock(String key) {
    return locks.computeIfAbsent(key, (_) -> new KeyLock());
  }

  public String getString(String key) {
    logger.debug(clock.instant().toString());
    logger.debug("Getting: {}", key);

    StoreEntry entry = map.get(key);

    logger.debug("Got: " + entry);

    if (!(entry instanceof StringEntry stringEntry)) {
      return null;
    }

    if (stringEntry.isExpired(clock)) {
      logger.debug("Entry " + key + " is expired");
      map.remove(key);
      return null;
    }

    return stringEntry.value();
  }

  public void setString(String key, String value) {
    map.put(key, new StringEntry(value, Optional.empty()));
  }

  public void setString(String key, String value, Duration expiry) {
    map.put(key, new StringEntry(value, Optional.of(clock.instant().plus(expiry))));
  }

  public SortedMap<StreamId, Stream> getStreams(String key) {
    KeyLock lock = getKeyLock(key);
    lock.lock();
    try {
      StoreEntry entry = map.get(key);

      if (!(entry instanceof StreamEntry streamEntry))
        throw new IllegalArgumentException(key + " is not a stream");

      return streamEntry.getValues();
    } finally {
      lock.unlock();
    }
  }

  public SortedMap<StreamId, Stream> getTailStreams(
      String key, PartialStreamId fromKey, boolean inclusive) {
    KeyLock lock = getKeyLock(key);
    lock.lock();
    try {
      StoreEntry entry = map.get(key);

      if (!(entry instanceof StreamEntry streamEntry))
        throw new IllegalArgumentException(key + " is not a stream");

      return streamEntry.getValues().tailMap(fromKey.from(), inclusive);
    } finally {
      lock.unlock();
    }
  }

  public SortedMap<StreamId, Stream> awaitTailStreams(
      String key, PartialStreamId fromKey, boolean inclusive, Instant timeout) {
    KeyLock lock = getKeyLock(key);
    long remainingNanos = Duration.between(Instant.now(), timeout).toNanos();
    try {
      if (lock.tryLock(remainingNanos)) {
        remainingNanos = Duration.between(Instant.now(), timeout).toNanos();
        while (!map.containsKey(key)) {
          remainingNanos = lock.awaitNanos(remainingNanos);
          if (remainingNanos < 0) {
            return null;
          }
        }
        StoreEntry entry = map.get(key);

        if (!(entry instanceof StreamEntry streamEntry))
          throw new IllegalArgumentException(key + " is not a stream");

        SortedMap<StreamId, Stream> result =
            streamEntry.getValues().tailMap(fromKey.from(), inclusive);

        while ((result = streamEntry.getValues().tailMap(fromKey.from(), inclusive)).isEmpty()) {
          remainingNanos = lock.awaitNanos(remainingNanos);
          if (remainingNanos < 0) {
            return null;
          }
        }

        return result;
      } else {
        return null;
      }
    } catch (InterruptedException e) {
      return null;
    } finally {
      lock.unlock();
    }
  }

  public SortedMap<StreamId, Stream> getRangeStreams(
      String key, PartialStreamId fromKey, PartialStreamId toKey) {
    KeyLock lock = getKeyLock(key);
    lock.lock();
    try {
      StoreEntry entry = map.get(key);

      if (!(entry instanceof StreamEntry streamEntry))
        throw new IllegalArgumentException(key + " is not a stream");

      return streamEntry.getValues().subMap(fromKey.from(), true, toKey.to(), true);
    } finally {
      lock.unlock();
    }
  }

  public StreamEntry getStreamEntry(String key) {
    KeyLock lock = getKeyLock(key);
    lock.lock();
    try {
      logger.debug("Getting: {}", key);

      StoreEntry entry = map.get(key);

      logger.debug("Got: " + entry);

      if (!(entry instanceof StreamEntry streamEntry))
        throw new IllegalArgumentException(key + " is not a stream");

      return streamEntry;
    } finally {
      lock.unlock();
    }
  }

  public String setStreamEntry(String key, String id, List<String> values) {
    KeyLock lock = getKeyLock(key);
    lock.lock();
    try {
      logger.debug("Adding streams to {}", key);
      StoreEntry entry = map.computeIfAbsent(key, (_) -> new StreamEntry());

      if (!(entry instanceof StreamEntry streamEntry))
        throw new IllegalArgumentException(key + " is not a stream");

      String generatedID = streamEntry.add(id, values, clock).toString();

      lock.signalAll();

      return generatedID;
    } finally {
      lock.unlock();
    }
  }

  public String type(String key) {
    StoreEntry entry = map.get(key);

    if (entry == null) return "none";

    return entry.type();
  }
}
