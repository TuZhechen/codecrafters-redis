package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

public class MultiImpl implements RedisCommandHandler {

    @Override
    public String invoke(String[] args, ClientHandler clientHandler, boolean invokeFromExec) {
        clientHandler.setInTransaction(true);
        clientHandler.getCommandQueue().clear();
        String response = RESPEncoder.encodeSimpleString("OK");
        if (!invokeFromExec) {
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
        }
        return response;
    }
}
