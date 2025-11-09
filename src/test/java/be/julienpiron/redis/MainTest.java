package be.julienpiron.redis;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import net.datafaker.Faker;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class MainTest {
  private TestStore store;
  private TestServer server;
  private Thread serverThread;
  private Faker faker = new Faker();

  @BeforeEach
  void startServer() throws IOException {
    store = new TestStore();
    server = new TestServer();
    serverThread =
        new Thread(
            () -> {
              server.setStore(store);
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
  void shouldHandlePING() throws Exception {
    String response = run(client -> client.send("PING"));
    assertEquals("+PONG\r\n", response);
  }

  @Test
  void shouldHandleMultiplePINGs() throws Exception {
    List<String> responses =
        run(
            client -> {
              String response1 = client.send("PING");
              String response2 = client.send("PING");
              return List.of(response1, response2);
            });

    assertEquals("+PONG\r\n", responses.get(0));
    assertEquals("+PONG\r\n", responses.get(1));
  }

  @Test
  void shouldHandleMultipleClients() throws Exception {
    String[] responses =
        run(
            (client1, client2) -> {
              return new String[] {client1.send("PING"), client2.send("PING")};
            });

    assertEquals("+PONG\r\n", responses[0]);
    assertEquals("+PONG\r\n", responses[1]);
  }

  @Test
  void shouldHandleECHO() throws Exception {
    String payload = faker.hobbit().character();

    String response = run(client -> client.send("ECHO", payload));

    assertEquals("$" + payload.length() + "\r\n" + payload + "\r\n", response);
  }

  @Test
  void shouldHandleSETandGET() throws Exception {
    String spell = faker.harryPotter().spell();

    String setResponse = run(client -> client.send("SET", "spell", spell));
    assertEquals("+OK\r\n", setResponse);

    String getResponse = run(client -> client.send("GET", "spell"));
    assertEquals("$" + spell.length() + "\r\n" + spell + "\r\n", getResponse);
  }

  @Test
  void shouldHandleSETwithEX() throws Exception {
    String spell = faker.harryPotter().spell();

    String setResponse = run(client -> client.send("SET", "spell", spell, "EX", "2.5"));
    assertEquals("+OK\r\n", setResponse);

    store.advanceClock(Duration.ofSeconds(2));
    String responseBeforeExpiry = run(client -> client.send("GET", "spell"));
    assertEquals("$" + spell.length() + "\r\n" + spell + "\r\n", responseBeforeExpiry);

    store.advanceClock(Duration.ofSeconds(1));
    String responseAfterExpiry = run(client -> client.send("GET", "spell"));
    assertEquals("$-1\r\n", responseAfterExpiry);
  }

  @Test
  void shouldHandleSETwithPX() throws Exception {
    String spell = faker.harryPotter().spell();

    String setResponse = run(client -> client.send("SET", "spell", spell, "PX", "500"));
    assertEquals("+OK\r\n", setResponse);

    store.advanceClock(Duration.ofMillis(200));
    String responseBeforeExpiry = run(client -> client.send("GET", "spell"));
    assertEquals("$" + spell.length() + "\r\n" + spell + "\r\n", responseBeforeExpiry);

    store.advanceClock(Duration.ofMillis(400));
    String responseAfterExpiry = run(client -> client.send("GET", "spell"));
    assertEquals("$-1\r\n", responseAfterExpiry);
  }

  @Test
  void shouldHandleTYPE() throws Exception {
    String character = faker.harryPotter().character();

    run(client -> client.send("SET", "favourite_character", character));
    assertEquals("+string\r\n", run(client -> client.send("TYPE", "favourite_character")));

    String response2 = run(client -> client.send("TYPE", "missing_key"));
    assertEquals("+none\r\n", response2);

    run(client -> client.send("XADD", "ennemies", "0-1", "name", character));
    assertEquals("+stream\r\n", run(client -> client.send("TYPE", "ennemies")));
  }

  @Test
  void shouldHandleXADD() throws Exception {
    String title = faker.harryPotter().book();
    String pages = Integer.toString(faker.number().numberBetween(100, 999));

    assertEquals(
        "$3\r\n0-1\r\n",
        run(client -> client.send("XADD", "books", "0-1", "title", title, "pages", pages)));

    assertEquals("+stream\r\n", run(client -> client.send("TYPE", "books")));
  }

  @Test
  void shouldReject00StreamID() throws Exception {
    assertEquals(
        "-ERR The ID specified in XADD must be greater than 0-0\r\n",
        run(client -> client.send("XADD", "books", "0-0", "title", "Deathly Hallows")));

    run(client -> client.send("XADD", "moovie", "0-1", "title", "Deathly Hallows"));
    assertEquals(
        "-ERR The ID specified in XADD must be greater than 0-0\r\n",
        run(client -> client.send("XADD", "moovie", "0-0", "title", "Deathly Hallows")));
  }

  @Test
  void shouldOnlyAcceptIncrementalStreamIDs() throws Exception {
    assertEquals(
        "$3\r\n1-1\r\n",
        run(
            client ->
                client.send(
                    "XADD", "books", "1-1", "title", "Philosopher's Stone", "year", "1997")));

    assertEquals(
        "$3\r\n1-2\r\n",
        run(
            client ->
                client.send(
                    "XADD", "books", "1-2", "title", "Chamber of Secrets", "year", "1998")));

    // The exact time and sequence number as the last entryThe exact time and sequence number as the
    // last entry
    assertEquals(
        "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n",
        run(
            client ->
                client.send(
                    "XADD", "books", "1-2", "title", "Chamber of Secrets", "year", "1998")));

    // A smaller value for the time and a larger value for the sequence number
    assertEquals(
        "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n",
        run(
            client ->
                client.send(
                    "XADD", "books", "0-3", "title", "Prisoner of Azkaban", "year", "1999")));
  }

  @ParameterizedTest
  @CsvSource({"'0-*', '$3\r\n0-1\r\n'", "'9-*', '$3\r\n9-0\r\n'"})
  void shouldAutoGenerateStreamIDSequenceWhenCreatingStream(String partialID, String generatedID)
      throws Exception {
    assertEquals(
        generatedID,
        run(client -> client.send("XADD", faker.harryPotter().house(), partialID, "foo", "bar")));
  }

  @ParameterizedTest
  @CsvSource({"'1997-*', '$6\r\n1997-3\r\n'", "'2000-*', '$6\r\n2000-0\r\n'"})
  void shouldAutoGenerateStreamIDSequenceWhenAddingToAnExistingStream(
      String partialID, String generatedID) throws Exception {
    String key = faker.harryPotter().location();

    run(client -> client.send("XADD", key, "1997-2", "foo", "bar"));

    assertEquals(generatedID, run(client -> client.send("XADD", key, partialID, "foo", "bar")));
  }

  @Test
  void shouldAutoGenerateStreamID() throws Exception {
    assertEquals(
        "$14\r\n852033600000-0\r\n",
        run(client -> client.send("XADD", "key", "*", "place", faker.harryPotter().location())));
    assertEquals(
        "$14\r\n852033600000-1\r\n",
        run(client -> client.send("XADD", "key", "*", "place", faker.harryPotter().location())));

    store.advanceClock(Duration.ofSeconds(10));

    assertEquals(
        "$14\r\n852033610000-0\r\n",
        run(client -> client.send("XADD", "key", "*", "place", faker.harryPotter().location())));
  }

  @Test
  void shouldHandleXRANGE() throws Exception {
    run(
        client ->
            client.send(
                "XADD", "some_key", "1526985054069-0", "temperature", "36", "humidity", "95"));
    run(
        client ->
            client.send(
                "XADD", "some_key", "1526985054069-1", "temperature", "36", "humidity", "95"));
    run(
        client ->
            client.send(
                "XADD", "some_key", "1526985054079-0", "temperature", "37", "humidity", "94"));
    run(
        client ->
            client.send(
                "XADD", "some_key", "1526985054089-0", "temperature", "37", "humidity", "94"));
    String response =
        run(client -> client.send("XRANGE", "some_key", "1526985054069-1", "1526985054079"));

    assertEquals(
        "*2\r\n"
            + "*2\r\n"
            + "$15\r\n1526985054069-1\r\n"
            + "*4\r\n"
            + "$11\r\ntemperature\r\n"
            + "$2\r\n36\r\n"
            + "$8\r\nhumidity\r\n"
            + "$2\r\n95\r\n"
            + "*2\r\n"
            + "$15\r\n1526985054079-0\r\n"
            + "*4\r\n"
            + "$11\r\ntemperature\r\n"
            + "$2\r\n37\r\n"
            + "$8\r\nhumidity\r\n"
            + "$2\r\n94\r\n",
        response);
  }

  private <T> T run(Function<TestClient, T> action) throws Exception {
    CompletableFuture<T> future =
        CompletableFuture.supplyAsync(
            () -> {
              try (TestClient client = new TestClient(server)) {
                return action.apply(client);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    return future.get(200, MILLISECONDS);
  }

  private <T> T run(BiFunction<TestClient, TestClient, T> action) throws Exception {
    CompletableFuture<T> future =
        CompletableFuture.supplyAsync(
            () -> {
              try (TestClient client1 = new TestClient(server);
                  TestClient client2 = new TestClient(server); ) {
                return action.apply(client1, client2);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    return future.get(200, MILLISECONDS);
  }
}
