import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Logger;

public class Server {
  private final int port;
  private final Logger logger = Logger.getLogger("Redis");

  private ServerSocket serverSocket = null;
  private Socket clientSocket = null;
  private boolean running = false;

  public Server(int port) {
    this.port = port;
  }

  public void start() {

    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);

      logger.info("Server started");
      running = true;

      clientSocket = serverSocket.accept();
      logger.info("New client connexion: " + clientSocket.getPort());

      PrintWriter writer = new PrintWriter(clientSocket.getOutputStream());
      writer.print("+PONG\r\n");
      writer.flush();

    } catch (IOException e) {
      logger.severe("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) {
        logger.severe("IOException: " + e.getMessage());
      }
    }
  }

  public void stop() {
    if (serverSocket != null && !serverSocket.isClosed()) {
      try {
        serverSocket.close();
      } catch (IOException e) {
        e.printStackTrace();
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
