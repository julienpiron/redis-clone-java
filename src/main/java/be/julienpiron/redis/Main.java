package be.julienpiron.redis;

public class Main {
  public static void main(String[] args) {
    Server server = new Server(6379);
    server.start();
  }
}
