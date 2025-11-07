import java.io.BufferedReader;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommandReader {
  private final BufferedReader reader;
  private final Logger logger = LoggerFactory.getLogger(CommandReader.class);

  CommandReader(BufferedReader reader) {
    this.reader = reader;
  }

  Command read() throws IOException {
    int type = reader.read();

    if (type == -1) {
      return null;
    }

    if (type != '*') {
      throw new IllegalArgumentException("Unknown type: " + (char) type);
    }

    int lenght = Integer.parseInt(reader.readLine());

    List<String> parts = new ArrayList<>();

    for (int i = 0; i < lenght; i++) {
      parts.add(readString());
    }

    logger.debug("Parts: " + parts);

    return switch (parts.get(0).toUpperCase()) {
      case "PING" -> new PingCommand();
      case "ECHO" -> new EchoCommand(parts.get(1));
      case "SET" -> {
        Optional<Duration> expiry;

        if (parts.size() == 5) {
          double duration = Double.parseDouble(parts.get(4));

          expiry =
              switch (parts.get(3)) {
                case "EX" -> Optional.of(Duration.ofNanos((int) (duration * 1_000_000_000)));
                case "PX" -> Optional.of(Duration.ofNanos((int) (duration * 1_000_000)));
                default -> throw new IllegalArgumentException();
              };
        } else {
          expiry = Optional.empty();
        }

        yield new SetCommand(parts.get(1), parts.get(2), expiry);
      }
      case "GET" -> new GetCommand(parts.get(1));
      default -> throw new IllegalArgumentException("Unknown command: " + parts.get(0));
    };
  }

  private String readString() throws IOException {
    int type = reader.read();

    if (type != '$') {
      throw new IllegalArgumentException("BulkString must start with an '$'");
    }

    int length = Integer.parseInt(reader.readLine());

    char[] buffer = new char[length];
    reader.read(buffer);
    reader.readLine(); // read CRLF

    return new String(buffer);
  }
}
