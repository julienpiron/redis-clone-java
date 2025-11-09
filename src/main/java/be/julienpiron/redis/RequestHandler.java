package be.julienpiron.redis;

import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestHandler {
  private Logger logger = LoggerFactory.getLogger(RequestHandler.class);
  private Request request;
  private Store store;
  private Clock clock;

  public RequestHandler(Request request, Store store, Clock clock) {
    this.request = request;
    this.store = store;
    this.clock = clock;
  }

  public RESPDataType handle() throws InvalidRequestException {
    logger.debug("Handling request: {}", request);

    return switch (request.command()) {
      case "ECHO" -> echo();
      case "GET" -> get();
      case "PING" -> ping();
      case "TYPE" -> type();
      case "SET" -> set();
      case "XADD" -> xadd();
      default -> throw new InvalidRequestException("Unknown command: " + request.command());
    };
  }

  private RESPDataType echo() throws InvalidRequestException {
    return new RESP.BulkString(request.argAsString(0));
  }

  private RESPDataType get() throws InvalidRequestException {
    String value = store.getString(request.argAsString(0), clock);

    if (value == null) return new RESP.BulkString(null);

    return new RESP.BulkString(value);
  }

  private RESPDataType ping() {
    return new RESP.SimpleString("PONG");
  }

  private RESPDataType type() throws InvalidRequestException {
    String key = request.argAsString(0);
    return new RESP.SimpleString(store.type(key));
  }

  private RESPDataType set() throws InvalidRequestException {
    String key = request.argAsString(0);
    String value = request.argAsString(1);

    if (request.args().size() == 2) {
      store.setString(key, value);
      return new RESP.SimpleString("OK");
    }

    enum ExpiryType {
      EX,
      PX;
    }
    ExpiryType expiryType = request.argAsEnum(2, ExpiryType.class);
    Double expiryOffset = request.argAsDouble(3);

    store.setString(
        key,
        value,
        switch (expiryType) {
          case EX -> Instant.now(clock).plusNanos((long) (expiryOffset * 1_000_000_000));
          case PX -> Instant.now(clock).plusNanos((long) (expiryOffset * 1_000_000));
        });

    return new RESP.SimpleString("OK");
  }

  private RESPDataType xadd() throws InvalidRequestException {
    try {

      String key = request.argAsString(0);
      String id = request.argAsString(1);

      String[] idParts = id.split("-");
      long milliseconds = Long.parseLong(idParts[0]);
      int sequence = Integer.parseInt(idParts[1]);

      Map<String, String> value = new HashMap<>();

      for (int i = 2; i + 1 < request.args().size(); i += 2) {
        value.put(request.argAsString(i), request.argAsString(i + 1));
      }

      store.setStream(key, new Stream(milliseconds, sequence, value));

      return new RESP.BulkString(id);
    } catch (IllegalArgumentException e) {
      return new RESP.SimpleError(e.getMessage());
    }
  }
}
