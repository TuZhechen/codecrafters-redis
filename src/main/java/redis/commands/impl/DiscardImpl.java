package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.commands.TransactionHelper;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

public class DiscardImpl implements RedisCommandHandler {

    @Override
    public String invoke(String[] args, ClientHandler clientHandler, boolean invokeFromExec) {
        String response;

        if(!clientHandler.isInTransaction()) {
            response = "ERR DISCARD without MULTI";
            return TransactionHelper.errorResponse(clientHandler, response, invokeFromExec);
        }

        clientHandler.setInTransaction(false);
        clientHandler.getCommandQueue().clear();
        response = RESPEncoder.encodeSimpleString("OK");
        if (!invokeFromExec) {
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
        }
        return response;
    }
}

