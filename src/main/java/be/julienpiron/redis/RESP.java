package be.julienpiron.redis;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

interface RESPDataType {
  public String encode();
}

public abstract class RESP {
  public static String CTRL = "\r\n";

  public record BulkString(String value) implements RESPDataType {
    public String encode() {
      if (value == null) return "$-1" + CTRL;

      return "$" + value.length() + CTRL + value + CTRL;
    }
  }

  public record SimpleString(String value) implements RESPDataType {
    public String encode() {
      return "+" + value + CTRL;
    }
  }

  public record Array(List<RESPDataType> items) implements RESPDataType {
    public Array(String... args) {
      this(Arrays.stream(args).map(BulkString::new).<RESPDataType>map(b -> b).toList());
    }

    public String encode() {
      return "*"
          + items.size()
          + CTRL
          + items.stream().map(item -> item.encode()).collect(Collectors.joining());
    }
  }
}
