import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Server {
  private final int port;
  private final Logger logger;

  private ServerSocket serverSocket = null;
  private boolean running = false;

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
  }

  public void start() {
    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);

      logger.info("Server started");
      running = true;

      while (running) {
        Socket clientSocket = serverSocket.accept();
        logger.info("New client connexion: " + clientSocket.getPort());
        new Thread(() -> handleClien(clientSocket)).start();
      }

    } catch (IOException e) {
      logger.info("IOException: " + e.getMessage());
    }
  }

  private void handleClien(Socket clientSocket) {
    try {
      PrintWriter writer = new PrintWriter(clientSocket.getOutputStream());
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

      char[] request = new char[1024];

      while ((reader.read(request)) != -1) {
        logger.info("Request: " + new String(request));
        writer.print("+PONG\r\n");
        writer.flush();
        request = new char[1024];
      }
    } catch (IOException e) {
      logger.warning("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
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
