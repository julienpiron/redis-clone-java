import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.ConsoleHandler;
import java.util.logging.Logger;

public class Main {
  public static void main(String[] args) {
    Logger logger = Logger.getLogger("Redis");
    logger.addHandler(new ConsoleHandler());

    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = 6379;

    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);

      logger.info("Server started");

      clientSocket = serverSocket.accept();
      logger.info("New client connexion: " + clientSocket.getPort());

      PrintWriter writer = new PrintWriter(clientSocket.getOutputStream());
      writer.print("+PONG\r\n");
      writer.flush();

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }
}
