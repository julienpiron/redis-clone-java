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

  public String ping() throws IOException {
    writer.write("*1\r\n$4\r\nPING\r\n");
    writer.flush();

    return getResponse();
  }

  public String echo(String value) throws IOException {
    writer.write("*2\r\n$4\r\nECHO\r\n$" + value.length() + "\r\n" + value + "\r\n");
    writer.flush();

    return getResponse();
  }

  public String set(String key, String value) throws IOException {
    writer.write(
        "*3\r\n$3\r\nSET\r\n$"
            + key.length()
            + "\r\n"
            + key
            + "\r\n$"
            + value.length()
            + "\r\n"
            + value
            + "\r\n");
    writer.flush();

    return getResponse();
  }

  public String get(String key) throws IOException {
    writer.write("*2\r\n$3\r\nGET\r\n$" + key.length() + "\r\n" + key + "\r\n");
    writer.flush();

    return getResponse();
  }

  private String getResponse() throws IOException {
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
