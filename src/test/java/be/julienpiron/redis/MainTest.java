package be.julienpiron.redis;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import net.datafaker.Faker;
import org.junit.jupiter.api.*;

public class MainTest {
  private TestServer server;
  private Thread serverThread;
  private Faker faker = new Faker();

  @BeforeEach
  void startServer() throws IOException {
    server = new TestServer();
    serverThread =
        new Thread(
            () -> {
              server.start();
            });
    serverThread.start();

    await().atMost(200, MILLISECONDS).until(() -> server.isRunning());
  }

  @AfterEach
  void stopServer() {
    server.stop();
  }

  @Test
  void shouldHandlePING() {
    AtomicReference<String> response = new AtomicReference<>();

    new Thread(
            () -> {
              try (TestClient client = new TestClient(server); ) {
                response.set(client.ping());
              } catch (Exception e) {
                fail(e);
              }
            })
        .start();

    await().atMost(200, MILLISECONDS).until(() -> response.get() != null);

    assertEquals("+PONG\r\n", response.get());
  }

  @Test
  void shouldHandleMultiplePINGs() {
    AtomicReference<String> response1 = new AtomicReference<>();
    AtomicReference<String> response2 = new AtomicReference<>();

    new Thread(
            () -> {
              try (TestClient client = new TestClient(server); ) {
                response1.set(client.ping());
                response2.set(client.ping());
              } catch (Exception e) {
                fail(e);
              }
            })
        .start();

    await()
        .atMost(200, MILLISECONDS)
        .until(() -> response1.get() != null && response2.get() != null);

    assertEquals("+PONG\r\n", response1.get());
    assertEquals("+PONG\r\n", response1.get());
  }

  @Test
  void shouldHandleMultipleClients() {
    AtomicReference<String> response1 = new AtomicReference<>();
    AtomicReference<String> response2 = new AtomicReference<>();

    new Thread(
            () -> {
              try (TestClient client1 = new TestClient(server);
                  TestClient client2 = new TestClient(server); ) {
                response1.set(client1.ping());
                response2.set(client2.ping());
              } catch (Exception e) {
                fail(e);
              }
            })
        .start();

    await()
        .atMost(200, MILLISECONDS)
        .until(() -> response1.get() != null && response2.get() != null);

    assertEquals("+PONG\r\n", response1.get());
    assertEquals("+PONG\r\n", response1.get());
  }

  @Test
  void shouldHandleECHO() {
    String payload = faker.hobbit().character();
    AtomicReference<String> response = new AtomicReference<>();

    new Thread(
            () -> {
              try (TestClient client = new TestClient(server); ) {
                response.set(client.echo(payload));
              } catch (Exception e) {
                fail(e);
              }
            })
        .start();

    await().atMost(200, MILLISECONDS).until(() -> response.get() != null);

    assertEquals("$" + payload.length() + "\r\n" + payload + "\r\n", response.get());
  }

  @Test
  void shouldHandleSETandGET() {
    String spell = faker.harryPotter().spell();

    AtomicReference<String> setResponse = new AtomicReference<>();
    AtomicReference<String> getResponse = new AtomicReference<>();

    new Thread(
            () -> {
              try (TestClient client = new TestClient(server); ) {
                setResponse.set(client.set("spell", spell));
                getResponse.set(client.get("spell"));
              } catch (Exception e) {
                fail(e);
              }
            })
        .start();

    await()
        .atMost(200, MILLISECONDS)
        .until(() -> setResponse.get() != null && getResponse.get() != null);

    assertEquals("+OK\r\n", setResponse.get());
    assertEquals("$" + spell.length() + "\r\n" + spell + "\r\n", getResponse.get());
  }

  @Test
  void shouldHandleSETwithEX() {
    String spell = faker.harryPotter().spell();

    AtomicReference<String> setResponse = new AtomicReference<>();
    AtomicReference<String> responseBeforeExpiry = new AtomicReference<>();
    AtomicReference<String> responseAfterExpiry = new AtomicReference<>();

    new Thread(
            () -> {
              try (TestClient client = new TestClient(server); ) {
                setResponse.set(client.set("spell", spell, "EX", "2.5"));
                server.advanceClock(Duration.ofSeconds(2));
                responseBeforeExpiry.set(client.get("spell"));
                server.advanceClock(Duration.ofSeconds(1));
                responseAfterExpiry.set(client.get("spell"));
              } catch (Exception e) {
                fail(e);
              }
            })
        .start();

    await()
        .atMost(200, MILLISECONDS)
        .until(() -> responseBeforeExpiry.get() != null && responseAfterExpiry.get() != null);

    assertEquals("$" + spell.length() + "\r\n" + spell + "\r\n", responseBeforeExpiry.get());
    assertEquals("$-1\r\n", responseAfterExpiry.get());
  }

  @Test
  void shouldHandleSETwithPX() {
    String spell = faker.harryPotter().spell();

    AtomicReference<String> setResponse = new AtomicReference<>();
    AtomicReference<String> responseBeforeExpiry = new AtomicReference<>();
    AtomicReference<String> responseAfterExpiry = new AtomicReference<>();

    new Thread(
            () -> {
              try (TestClient client = new TestClient(server); ) {
                setResponse.set(client.set("spell", spell, "PX", "500"));
                server.advanceClock(Duration.ofMillis(200));
                responseBeforeExpiry.set(client.get("spell"));
                server.advanceClock(Duration.ofMillis(400));
                responseAfterExpiry.set(client.get("spell"));
              } catch (Exception e) {
                fail(e);
              }
            })
        .start();

    await()
        .atMost(200, MILLISECONDS)
        .until(() -> responseBeforeExpiry.get() != null && responseAfterExpiry.get() != null);

    assertEquals("$" + spell.length() + "\r\n" + spell + "\r\n", responseBeforeExpiry.get());
    assertEquals("$-1\r\n", responseAfterExpiry.get());
  }
}
