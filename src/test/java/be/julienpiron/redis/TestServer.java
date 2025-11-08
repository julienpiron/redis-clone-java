package be.julienpiron.redis;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

class TestServer extends Server {
  TestServer() throws IOException {
    super(TestServer.getRandomPort());

    clock = Clock.fixed(Instant.parse("1996-12-31T12:00:00.00Z"), ZoneId.systemDefault());
  }

  public void advanceClock(Duration duration) {
    clock = Clock.offset(clock, duration);
  }

  private static int getRandomPort() throws IOException {
    try (ServerSocket serverSocket = new ServerSocket(0); ) {
      return serverSocket.getLocalPort();
    }
  }
}
