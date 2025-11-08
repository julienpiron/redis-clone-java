package be.julienpiron.redis;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class RequestParser {
  private final BufferedReader reader;

  RequestParser(BufferedReader reader) {
    this.reader = reader;
  }

  Request read() throws IOException {
    int type = reader.read();

    if (type == -1) {
      return null;
    }

    if (type != '*') {
      throw new IllegalArgumentException("Unknown type: " + (char) type);
    }

    int lenght = Integer.parseInt(reader.readLine());

    String command = readString();

    List<String> args = new ArrayList<>();

    for (int i = 1; i < lenght; i++) {
      args.add(readString());
    }

    Request request = new Request(command, args);

    return request;
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
