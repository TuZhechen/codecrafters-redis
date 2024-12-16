package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.commands.TransactionHelper;
import redis.core.StorageManager;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

public class KeysImpl implements RedisCommandHandler {
    private final StorageManager storageManager;

    public KeysImpl(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public String invoke(String[] args, ClientHandler clientHandler, boolean invokeFromExec) {
        String response;
        if (args.length < 2) {
            response = "ERR KEYS requires at least one pattern argument";
            return TransactionHelper.errorResponse(clientHandler, response, invokeFromExec);
        }

        if (TransactionHelper.isHandlingTransaction(clientHandler, args)) return null;

        String pattern = args[1];
        String regex = pattern.replace("*", ".*");
        String[] matchedKeys = storageManager.keySet().stream()
                .filter(key -> key.matches(regex))
                .toArray(String[]::new);
        response = RESPEncoder.encodeArray(
                matchedKeys
        );
        if (!invokeFromExec) {
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
        }
        return response;
    }
}
