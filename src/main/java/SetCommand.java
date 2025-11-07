import java.time.Duration;
import java.util.Optional;

record SetCommand(String key, String value, Optional<Duration> expiry) implements Command {}
