package be.julienpiron.redis;

import java.io.IOException;
import java.net.ServerSocket;

public class TestServer extends Server {
  TestServer() throws IOException {
    super(TestServer.getRandomPort());
  }

  public void setStore(TestStore store) {
    this.store = store;
  }

  private static int getRandomPort() throws IOException {
    try (ServerSocket serverSocket = new ServerSocket(0); ) {
      return serverSocket.getLocalPort();
    }
  }
}
