package be.julienpiron.redis;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestHandler {
  private Logger logger = LoggerFactory.getLogger(RequestHandler.class);
  private Request request;
  private Store store;

  public RequestHandler(Request request, Store store) {
    this.request = request;
    this.store = store;
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
      case "XRANGE" -> xrange();
      default -> throw new InvalidRequestException("Unknown command: " + request.command());
    };
  }

  private RESPDataType echo() throws InvalidRequestException {
    return new RESP.BulkString(request.argAsString(0));
  }

  private RESPDataType get() throws InvalidRequestException {
    String value = store.getString(request.argAsString(0));

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
          case EX -> Duration.ofNanos((long) (expiryOffset * 1_000_000_000));
          case PX -> Duration.ofNanos((long) (expiryOffset * 1_000_000));
        });

    return new RESP.SimpleString("OK");
  }

  private RESPDataType xadd() throws InvalidRequestException {
    try {
      String key = request.argAsString(0);
      String id = request.argAsString(1);

      String generatedId =
          store.setStream(key, id, request.args().subList(2, request.args().size()));

      return new RESP.BulkString(generatedId);
    } catch (IllegalArgumentException e) {
      return new RESP.SimpleError(e.getMessage());
    }
  }

  private RESPDataType xrange() throws InvalidRequestException {
    String key = request.argAsString(0);
    Stream.ID from =
        request.argAsString(1).equals("-")
            ? null
            : Stream.ID.parse(request.argAsString(1), store.clock);
    Stream.ID to =
        request.argAsString(2).equals("+")
            ? null
            : Stream.ID.parse(request.argAsString(2), store.clock);

    List<RESPDataType> filteredStreams = new ArrayList<>();

    for (Stream stream : store.getStream(key)) {
      if (from != null && stream.id().compareTo(from) < 0) {
        continue;
      }
      if (to != null && to.compareTo(stream.id()) < 0) {
        break;
      }

      filteredStreams.add(
          new RESP.Array(
              List.of(
                  new RESP.BulkString(stream.id().toString()),
                  new RESP.Array(
                      stream.values().stream()
                          .map(RESP.BulkString::new)
                          .collect(Collectors.toList())))));
    }

    return new RESP.Array(filteredStreams);
  }
}
