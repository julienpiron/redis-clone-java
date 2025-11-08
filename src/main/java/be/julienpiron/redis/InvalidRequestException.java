package be.julienpiron.redis;

public class InvalidRequestException extends Exception {
  public InvalidRequestException(String message) {
    super(message);
  }

  public InvalidRequestException(Throwable throwable) {
    super(throwable);
  }
}
