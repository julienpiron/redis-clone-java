import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Clock;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {
  protected final int port;
  protected final Logger logger = LoggerFactory.getLogger(Server.class);
  protected boolean running;
  protected ServerSocket serverSocket;
  protected Store store;
  protected Clock clock;

  public Server(int port) {
    this.port = port;
    this.store = new Store();
    this.clock = Clock.systemDefaultZone();
  }

  public void start() {
    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);

      logger.debug("Server started");
      running = true;

      while (running) {
        Socket client = serverSocket.accept();
        logger.debug("New client connexion: " + client.getPort());
        new Thread(() -> handleClient(client)).start();
      }
    } catch (IOException e) {
      logger.debug("IOException: " + e.getMessage());
    }
  }

  private void handleClient(Socket client) {
    try {
      PrintWriter writer = new PrintWriter(client.getOutputStream());
      RequestParser reader =
          new RequestParser(new BufferedReader(new InputStreamReader(client.getInputStream())));

      Request request;

      while ((request = reader.read()) != null) {
        try {

          String response =
              switch (request.command()) {
                case "ECHO" ->
                    "$"
                        + request.argAsString(0).length()
                        + "\r\n"
                        + request.argAsString(0)
                        + "\r\n";
                case "GET" -> {
                  String value = store.get(request.argAsString(0), clock);

                  if (value == null) yield "$-1\r\n";

                  yield "$" + value.length() + "\r\n" + value + "\r\n";
                }
                case "PING" -> "+PONG\r\n";
                case "SET" -> {
                  String key = request.argAsString(0);
                  String value = request.argAsString(1);

                  if (request.args().size() == 2) {
                    store.set(key, value);
                    yield "+OK\r\n";
                  }

                  enum ExpiryType {
                    EX,
                    PX;
                  }
                  ExpiryType expiryType = request.argAsEnum(2, ExpiryType.class);
                  Double expiryOffset = request.argAsDouble(3);

                  store.set(
                      key,
                      value,
                      switch (expiryType) {
                        case EX ->
                            Instant.now(clock).plusNanos((long) (expiryOffset * 1_000_000_000));
                        case PX -> Instant.now(clock).plusNanos((long) (expiryOffset * 1_000_000));
                      });

                  yield "+OK\r\n";
                }
                default -> throw new IllegalArgumentException("Unknown command");
              };

          writer.print(response);
          writer.flush();
        } catch (InvalidRequestException e) {
          logger.error("Invalid Request: " + e.getMessage());
        }
      }

    } catch (IOException e) {
      logger.error("IOException: " + e.getMessage());
    } finally {
      try {
        if (client != null) {
          client.close();
        }
      } catch (IOException e) {
        logger.error("IOException: " + e.getMessage());
      }
    }
  }

  public void stop() {
    if (serverSocket != null && !serverSocket.isClosed()) {
      try {
        serverSocket.close();
      } catch (IOException e) {
        logger.debug("IOException: " + e.getMessage());
      }
    }
    running = false;
  }

  public int getPort() {
    return port;
  }

  public boolean isRunning() {
    return running;
  }

  public Clock getClock() {
    return clock;
  }
}
