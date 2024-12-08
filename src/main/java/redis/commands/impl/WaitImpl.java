package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

public class WaitImpl implements RedisCommandHandler {
    @Override
    public void invoke(String[] args, ClientHandler clientHandler) {
        String response;
        if (args.length != 3) {
            response = "-ERR wrong number of arguments for 'WAIT'\r\n";
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
            return;
        }
        response = RESPEncoder.encodeInteger("0");
        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();
    }
}
