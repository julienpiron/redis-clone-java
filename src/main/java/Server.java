import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Server {
  private final int port;
  private final Logger logger;

  private boolean running;
  private ServerSocket serverSocket;
  private ConcurrentHashMap<String, String> store;

  public Server(int port) {
    this(port, Level.WARNING);
  }

  public Server(int port, Level loggingLevel) {
    this.port = port;

    this.logger = Logger.getLogger("Redis");
    logger.setLevel(loggingLevel);
    for (Handler handler : logger.getHandlers()) {
      handler.setLevel(loggingLevel);
    }

    this.store = new ConcurrentHashMap<>();
  }

  public void start() {
    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);

      logger.info("Server started");
      running = true;

      while (running) {
        Socket client = serverSocket.accept();
        logger.info("New client connexion: " + client.getPort());
        new Thread(() -> handleClient(client)).start();
      }
    } catch (IOException e) {
      logger.info("IOException: " + e.getMessage());
    }
  }

  private void handleClient(Socket client) {
    try {
      PrintWriter writer = new PrintWriter(client.getOutputStream());
      CommandReader reader =
          new CommandReader(new BufferedReader(new InputStreamReader(client.getInputStream())));

      Command command;

      while ((command = reader.read()) != null) {
        logger.info("Request: " + command);
        String response =
            switch (command) {
              case PingCommand _ -> "+PONG\r\n";
              case EchoCommand c -> "$" + c.value().length() + "\r\n" + c.value() + "\r\n";
              case SetCommand c -> {
                store.put(c.key(), c.value());
                yield "+OK\r\n";
              }
              case GetCommand c -> {
                String value = store.get(c.key());
                yield "$" + value.length() + "\r\n" + value + "\r\n";
              }
              default -> throw new IllegalArgumentException("Unknown command");
            };

        writer.print(response);
        writer.flush();
      }

    } catch (IOException e) {
      logger.warning("IOException: " + e.getMessage());
    } finally {
      try {
        if (client != null) {
          client.close();
        }
      } catch (IOException e) {
        logger.warning("IOException: " + e.getMessage());
      }
    }
  }

  public void stop() {
    if (serverSocket != null && !serverSocket.isClosed()) {
      try {
        serverSocket.close();
      } catch (IOException e) {
        logger.info("IOException: " + e.getMessage());
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
}
