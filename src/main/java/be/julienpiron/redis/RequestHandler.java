package be.julienpiron.redis;

import java.time.Clock;
import java.time.Instant;

public class RequestHandler {
  private Request request;
  private Store store;
  private Clock clock;

  public RequestHandler(Request request, Store store, Clock clock) {
    this.request = request;
    this.store = store;
    this.clock = clock;
  }

  public RESPDataType handle() throws InvalidRequestException {
    return switch (request.command()) {
      case "ECHO" -> echo();
      case "GET" -> get();
      case "PING" -> ping();
      case "SET" -> set();
      default -> throw new InvalidRequestException("Unknown command: " + request.command());
    };
  }

  private RESPDataType echo() throws InvalidRequestException {
    return new RESP.BulkString(request.argAsString(0));
  }

  private RESPDataType get() throws InvalidRequestException {
    String value = store.get(request.argAsString(0), clock);

    if (value == null) return new RESP.BulkString(null);

    return new RESP.BulkString(value);
  }

  private RESPDataType ping() {
    return new RESP.SimpleString("PONG");
  }

  private RESPDataType set() throws InvalidRequestException {
    String key = request.argAsString(0);
    String value = request.argAsString(1);

    if (request.args().size() == 2) {
      store.set(key, value);
      return new RESP.SimpleString("OK");
    }

    enum ExpiryType {
      EX,
      PX;
    }
    ExpiryType expiryType = request.argAsEnum(2, ExpiryType.class);
    Double expiryOffset = request.argAsDouble(3);

    store.set(
        key,
        value,
        switch (expiryType) {
          case EX -> Instant.now(clock).plusNanos((long) (expiryOffset * 1_000_000_000));
          case PX -> Instant.now(clock).plusNanos((long) (expiryOffset * 1_000_000));
        });

    return new RESP.SimpleString("OK");
  }
}
