package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.commands.TransactionHelper;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

public class EchoImpl implements RedisCommandHandler {
    @Override
    public String invoke(String[] args, ClientHandler clientHandler, boolean invokeFromExec) {
        if (TransactionHelper.isHandlingTransaction(clientHandler, args)) return null;

        String response;
        if (args.length > 1) {
            response = RESPEncoder.encodeBulkString(args[1]);
        } else {
            response = RESPEncoder.encodeErrorString("ERR wrong number of arguments for 'ECHO'");
        }
        if (!invokeFromExec) {
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
        }
        return response;
    }
}

