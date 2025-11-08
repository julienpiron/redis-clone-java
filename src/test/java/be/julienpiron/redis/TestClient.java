package be.julienpiron.redis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

class TestClient implements AutoCloseable {
  private Socket socket;
  private PrintWriter writer;
  private BufferedReader reader;

  public TestClient(Server server) throws UnknownHostException, IOException {
    socket = new Socket("localhost", server.getPort());
    writer = new PrintWriter(socket.getOutputStream());
    reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
  }

  public String send(String... args) throws IOException {
    RESPDataType data = new RESP.Array(args);

    writer.write(data.encode());
    writer.flush();

    char[] response = new char[2048];
    int length = reader.read(response);

    if (length <= 0) {
      return null;
    }

    return new String(response, 0, length);
  }

  @Override
  public void close() throws Exception {
    socket.close();
  }
}
