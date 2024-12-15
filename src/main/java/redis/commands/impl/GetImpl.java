package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.commands.TransactionHelper;
import redis.core.MortalValue;
import redis.core.StorageManager;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

public class GetImpl implements RedisCommandHandler {
    private final StorageManager storageManager;

    public GetImpl(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public String invoke(String[] args, ClientHandler clientHandler,boolean invokeFromExec) {
        String response;
        if (args.length < 2) {
            response = "-ERR wrong number of arguments for 'GET'\r\n";
            clientHandler.getWriter().print(response);
            return null;
        }

        if (TransactionHelper.isHandlingTransaction(clientHandler, args)) return null;

        String key = args[1];
        MortalValue<String> mortalValue = storageManager.get(key, String.class);

        if (mortalValue != null && !mortalValue.isExpired()) {
            response = RESPEncoder.encodeBulkString(mortalValue.getValue());
        } else {
            System.out.println("Found nothing");
            response = RESPEncoder.encodeBulkString("");
        }

        if (!invokeFromExec) {
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
        }
        return response;
    }
}
