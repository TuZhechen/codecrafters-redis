package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.protocol.RESP.parsers.SimpleStringImpl;
import redis.server.ClientHandler;

public class ReplConfImpl implements RedisCommandHandler {
    @Override
    public void invoke(String[] args, ClientHandler clientHandler) {
        String response = new SimpleStringImpl().encode("OK");
        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();
    }
}
