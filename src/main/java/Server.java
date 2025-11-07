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
      CommandReader reader =
          new CommandReader(new BufferedReader(new InputStreamReader(client.getInputStream())));

      Command command;

      while ((command = reader.read()) != null) {
        logger.debug("Request: " + command);
        logger.debug("Now: " + Instant.now(clock));
        String response =
            switch (command) {
              case PingCommand _ -> "+PONG\r\n";
              case EchoCommand c -> "$" + c.value().length() + "\r\n" + c.value() + "\r\n";
              case SetCommand c -> {
                if (c.expiry().isEmpty()) {
                  store.set(c.key(), c.value());
                } else {
                  store.set(c.key(), c.value(), Instant.now(clock).plus(c.expiry().get()));
                }
                yield "+OK\r\n";
              }
              case GetCommand c -> {
                String value = store.get(c.key(), clock);
                if (value == null) {
                  yield "$-1\r\n";
                }
                yield "$" + value.length() + "\r\n" + value + "\r\n";
              }
              default -> throw new IllegalArgumentException("Unknown command");
            };

        writer.print(response);
        writer.flush();
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
