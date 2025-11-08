package be.julienpiron.redis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Clock;
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
        logger.debug("Request: {}", request);
        try {
          RequestHandler handler = new RequestHandler(request, store, clock);

          String response = handler.handle();
          logger.debug("Response: {}", response);

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
