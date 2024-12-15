package redis.commands.impl;


import redis.commands.RedisCommandHandler;
import redis.commands.TransactionHelper;
import redis.core.MortalValue;
import redis.core.StorageManager;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

public class TypeImpl implements RedisCommandHandler {
    private final StorageManager storageManager;

    public TypeImpl(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public String invoke(String[] args, ClientHandler clientHandler, boolean invokeFromExec) {
        String response = null, key;
        if (args.length != 2) {
            response = "-ERR wrong number of arguments for 'TYPE'\r\n";
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
            return null;
        }

        if (TransactionHelper.isHandlingTransaction(clientHandler, args)) return null;

        key = args[1];
        MortalValue<?> mortalValue = storageManager.getRawValue(key);

        if (mortalValue == null) {
            response = RESPEncoder.encodeSimpleString("none");
        } else {
            response = RESPEncoder.encodeSimpleString(mortalValue.getType());
        }

        if (!invokeFromExec) {
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
        }
        return response;
    }
}