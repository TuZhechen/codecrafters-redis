package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.commands.TransactionHelper;
import redis.core.MortalValue;
import redis.core.StorageManager;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

public class IncrImpl implements RedisCommandHandler {
    private final StorageManager storageManager;

    public IncrImpl(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public String invoke(String[] args, ClientHandler clientHandler, boolean invokeFromExec) {
        String response;
        if (args.length < 2) {
            response = "ERR wrong number of arguments for 'INCR'";
            return TransactionHelper.errorResponse(clientHandler, response, invokeFromExec);
        }

        if (TransactionHelper.isHandlingTransaction(clientHandler, args)) return null;

        String key = args[1];
        MortalValue<?> mortalValue = storageManager.getRawValue(key);
        if (mortalValue == null) {
            mortalValue = new MortalValue<> ("1");
            storageManager.put(key, mortalValue);
            response = RESPEncoder.encodeInteger(1);
        } else {
            Object value = mortalValue.getValue();
            if (!(value instanceof String)) {
                response = "ERR target value cannot be parsed and increased";
                return TransactionHelper.errorResponse(clientHandler, response, invokeFromExec);
            }

            try {
                int number = Integer.parseInt((String) value);
                response = RESPEncoder.encodeInteger(++number);
                String newValue = String.valueOf(number);
                MortalValue<String> newMortalValue = new MortalValue<>(newValue);
                storageManager.put(key, newMortalValue);
            } catch (NumberFormatException e) {
                response = RESPEncoder.encodeErrorString("ERR value is not an integer or out of range");
            }

        }


        if (!invokeFromExec) {
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
        }
        return response;
    }
}