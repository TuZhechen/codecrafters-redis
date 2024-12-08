package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

public class PingImpl implements RedisCommandHandler {
    @Override
    public void invoke(String[] args, ClientHandler clientHandler) {
        String response;
        if (args.length == 1) {
            response = RESPEncoder.encodeSimpleString("PONG");
        } else {
            response = RESPEncoder.encodeSimpleString(args[1]);
        }

        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();
    }
}
