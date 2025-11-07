import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import org.junit.jupiter.api.*;

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

    char[] response = new char[7];
    reader.read(response);

    return new String(response);
  }

  @Override
  public void close() throws Exception {
    socket.close();
  }
}

public class MainTest {
  private static final Level LOGGING_LEVEL = Level.WARNING;
  private Server server;
  private Thread serverThread;

  @BeforeEach
  void startServer() throws IOException {
    server = new Server(getRandomPort(), LOGGING_LEVEL);
    serverThread =
        new Thread(
            () -> {
              server.start();
            });
    serverThread.start();

    await().atMost(200, MILLISECONDS).until(() -> server.isRunning());
  }

  @AfterEach
  void stopServer() {
    server.stop();
  }

  @Test
  void shouldHandlePING() {
    AtomicReference<String> response = new AtomicReference<>();

    new Thread(
            () -> {
              try (TestClient client = new TestClient(server); ) {
                response.set(client.ping());
              } catch (Exception e) {
                fail(e);
              }
            })
        .start();

    await().atMost(200, MILLISECONDS).until(() -> response.get() != null);

    assertEquals("+PONG\r\n", response.get());
  }

  @Test
  void shouldHandleMultiplePINGs() {
    AtomicReference<String> response1 = new AtomicReference<>();
    AtomicReference<String> response2 = new AtomicReference<>();

    new Thread(
            () -> {
              try (TestClient client = new TestClient(server); ) {
                response1.set(client.ping());
                response2.set(client.ping());
              } catch (Exception e) {
                fail(e);
              }
            })
        .start();

    await()
        .atMost(200, MILLISECONDS)
        .until(() -> response1.get() != null && response2.get() != null);

    assertEquals("+PONG\r\n", response1.get());
    assertEquals("+PONG\r\n", response1.get());
  }

  @Test
  void shouldHandleMultipleClients() {
    AtomicReference<String> response1 = new AtomicReference<>();
    AtomicReference<String> response2 = new AtomicReference<>();

    new Thread(
            () -> {
              try (TestClient client1 = new TestClient(server);
                  TestClient client2 = new TestClient(server); ) {
                response1.set(client1.ping());
                response2.set(client2.ping());
              } catch (Exception e) {
                fail(e);
              }
            })
        .start();

    await()
        .atMost(200, MILLISECONDS)
        .until(() -> response1.get() != null && response2.get() != null);

    assertEquals("+PONG\r\n", response1.get());
    assertEquals("+PONG\r\n", response1.get());
  }

  int getRandomPort() throws IOException {
    try (ServerSocket serverSocket = new ServerSocket(0); ) {
      return serverSocket.getLocalPort();
    }
  }
}
