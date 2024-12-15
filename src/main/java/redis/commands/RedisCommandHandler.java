package redis.commands;

import redis.server.ClientHandler;

import java.io.IOException;

public interface RedisCommandHandler {
    String invoke(String[] args, ClientHandler clientHandler, boolean invokeFromExec) throws IOException;
}
