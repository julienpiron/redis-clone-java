import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import org.junit.jupiter.api.Test;

public class MainTest {
  @Test
  void shouldWork() throws UnknownHostException, IOException {
    Thread runner =
        new Thread(
            () -> {
              Main.main(new String[0]);
            });
    runner.start();

    try (Socket client = new Socket("localhost", 6379); ) {
      PrintWriter writer = new PrintWriter(client.getOutputStream());
      writer.println("+PING\r\n");

      BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));

      char[] resultChar = new char[1024];
      int length = reader.read(resultChar);
      String result = new String(resultChar, 0, length);
      assertEquals("+PONG\r\n", result);
    }

    runner.interrupt();
  }
}
