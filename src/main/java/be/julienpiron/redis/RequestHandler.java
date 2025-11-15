package be.julienpiron.redis;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
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
      case "XREAD" -> xread();
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
          store.setStreamEntry(key, id, request.args().subList(2, request.args().size()));

      return new RESP.BulkString(generatedId);
    } catch (IllegalArgumentException e) {
      return new RESP.SimpleError(e.getMessage());
    }
  }

  private RESPDataType streamToRESP(Map.Entry<StreamId, Stream> entry) {
    return new RESP.Array(
        List.of(
            new RESP.BulkString(entry.getKey().toString()),
            new RESP.Array(
                entry.getValue().values().stream()
                    .map(RESP.BulkString::new)
                    .collect(Collectors.toList()))));
  }

  private RESPDataType xrange() throws InvalidRequestException {
    String key = request.argAsString(0);

    PartialStreamId from = PartialStreamId.parse(request.argAsString(1));
    PartialStreamId to = PartialStreamId.parse(request.argAsString(2));

    SortedMap<StreamId, Stream> filteredStreams = store.getRangeStreams(key, from, to);

    return new RESP.Array(
        filteredStreams.entrySet().stream().map(this::streamToRESP).collect(Collectors.toList()));
  }

  private RESPDataType xread() throws InvalidRequestException {
    if (request.argAsString(0).equalsIgnoreCase("BLOCK")) {
      return xreadBlock();
    }

    int numberOfStreams = (request.args().size() - 1) / 2;

    logger.debug("Number of streams: {}", numberOfStreams);

    List<RESPDataType> result = new ArrayList<>();

    for (int i = 1; i <= numberOfStreams; i++) {
      String key = request.argAsString(i);

      PartialStreamId fromKey = PartialStreamId.parse(request.argAsString(i + numberOfStreams));

      SortedMap<StreamId, Stream> filteredStreams = store.getTailStreams(key, fromKey, false);

      result.add(
          new RESP.Array(
              List.of(
                  new RESP.BulkString(key),
                  new RESP.Array(
                      filteredStreams.entrySet().stream()
                          .map(this::streamToRESP)
                          .collect(Collectors.toList())))));
    }
    return new RESP.Array(result);
  }

  private RESPDataType xreadBlock() throws InvalidRequestException {
    Instant timeout = Instant.now().plusMillis(request.argAsLong(1));
    String key = request.argAsString(3);
    PartialStreamId fromKey = PartialStreamId.parse(request.argAsString(4));

    SortedMap<StreamId, Stream> streams = store.awaitTailStreams(key, fromKey, false, timeout);

    if (streams == null) {
      return new RESP.Array();
    }

    return new RESP.Array(
        List.of(
            new RESP.Array(
                List.of(
                    new RESP.BulkString(key),
                    new RESP.Array(
                        streams.entrySet().stream()
                            .map(this::streamToRESP)
                            .collect(Collectors.toList()))))));
  }
}
