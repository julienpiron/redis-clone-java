import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class CommandReader {
  private BufferedReader reader;

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

    return switch (parts.get(0).toUpperCase()) {
      case "PING" -> new PingCommand();
      case "ECHO" -> new EchoCommand(parts.get(1));
      case "SET" -> new SetCommand(parts.get(1), parts.get(2));
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
