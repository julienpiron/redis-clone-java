package be.julienpiron.redis;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

interface RESPDataType {
  public String encode();
}

public abstract class RESP {
  public static String CRLF = "\r\n";

  public record BulkString(String value) implements RESPDataType {
    public String encode() {
      if (value == null) return "$-1" + CRLF;

      return "$" + value.length() + CRLF + value + CRLF;
    }
  }

  public record SimpleString(String value) implements RESPDataType {
    public String encode() {
      return "+" + value + CRLF;
    }
  }

  public record SimpleError(String message) implements RESPDataType {
    public String encode() {
      return "-" + message + CRLF;
    }
  }

  public record Array(List<RESPDataType> items) implements RESPDataType {
    public Array(String... args) {
      this(Arrays.stream(args).map(BulkString::new).<RESPDataType>map(b -> b).toList());
    }

    public String encode() {
      if (items.isEmpty()) {
        return "*-1" + CRLF;
      }
      return "*"
          + items.size()
          + CRLF
          + items.stream().map(item -> item.encode()).collect(Collectors.joining());
    }
  }
}
