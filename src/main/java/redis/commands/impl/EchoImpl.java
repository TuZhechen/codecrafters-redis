package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

public class EchoImpl implements RedisCommandHandler {
    @Override
    public void invoke(String[] args, ClientHandler clientHandler) {
        String response;
        if (args.length > 1) {
            response = RESPEncoder.encodeBulkString(args[1]);
        } else {
            response = "-ERR wrong number of arguments for 'ECHO'\n";
        }
        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();
    }
}

