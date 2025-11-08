package be.julienpiron.redis;

import java.util.List;

public record Request(String command, List<String> args) {
  public Request {
    command = command.toUpperCase();
  }

  public String command() {
    return command;
  }

  public List<String> args() {
    return args;
  }

  public String argAsString(int index) throws InvalidRequestException {
    try {
      return args.get(index);
    } catch (IndexOutOfBoundsException e) {
      throw new InvalidRequestException(e);
    }
  }

  public double argAsDouble(int index) throws InvalidRequestException {
    try {
      return Double.parseDouble(args.get(index));
    } catch (NumberFormatException | IndexOutOfBoundsException e) {
      throw new InvalidRequestException(e);
    }
  }

  public <E extends Enum<E>> E argAsEnum(int index, Class<E> enumClass)
      throws InvalidRequestException {
    try {
      return Enum.valueOf(enumClass, args.get(index));
    } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
      throw new InvalidRequestException(e);
    }
  }
}
