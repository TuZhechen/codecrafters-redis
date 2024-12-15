package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

public class MultiImpl implements RedisCommandHandler {

    @Override
    public String invoke(String[] args, ClientHandler clientHandler, boolean invokeFromExec) {
        String response;
        if (args.length > 1) {
            response = "-ERR wrong number of arguments for 'MULTI'\r\n";
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
            return null;
        }

        clientHandler.setInTransaction(true);
        clientHandler.getCommandQueue().clear();
        response = RESPEncoder.encodeSimpleString("OK");
        if (!invokeFromExec) {
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
        }
        return response;
    }
}
