package redis.commands;

import redis.server.ClientHandler;

import java.io.IOException;

public interface RedisCommandHandler {
    void invoke(String[] args, ClientHandler clientHandler) throws IOException;
}
