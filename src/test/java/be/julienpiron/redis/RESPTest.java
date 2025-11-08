package be.julienpiron.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

public class RESPTest {
  @Test
  void shouldEncodeBulkString() {
    RESP.BulkString bulkString = new RESP.BulkString("Harry Potter");

    assertEquals("$12\r\nHarry Potter\r\n", bulkString.encode());
  }

  @Test
  void shouldEncodeNullBulkString() {
    RESP.BulkString bulkString = new RESP.BulkString(null);

    assertEquals("$-1\r\n", bulkString.encode());
  }

  @Test
  void shouldEncodeSimpleString() {
    RESP.SimpleString simpleString = new RESP.SimpleString("OK");

    assertEquals("+OK\r\n", simpleString.encode());
  }

  @Test
  void shouldEncodeArray() {
    RESP.Array array =
        new RESP.Array(List.of(new RESP.SimpleString("Hello"), new RESP.BulkString("world!")));

    assertEquals("*2\r\n+Hello\r\n$6\r\nworld!\r\n", array.encode());
  }

  @Test
  void shouldCreateArrayFromListOfString() {
    RESP.Array array1 =
        new RESP.Array(List.of(new RESP.BulkString("Hello"), new RESP.BulkString("world")));
    RESP.Array array2 = new RESP.Array("Hello", "world");

    assertEquals(array1, array2);
  }
}
