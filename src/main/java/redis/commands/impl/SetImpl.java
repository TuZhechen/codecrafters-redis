package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.commands.TransactionHelper;
import redis.core.MortalValue;
import redis.core.StorageManager;
import redis.protocol.RESP.RESPEncoder;
import redis.replication.ReplicaManager;
import redis.server.ClientHandler;

public class SetImpl implements RedisCommandHandler {
    private final StorageManager storageManager;

    public SetImpl(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public String invoke(String[] args, ClientHandler clientHandler, boolean invokeFromExec) {
        String response;
        if (args.length < 3) {
            response = "ERR wrong number of arguments for 'SET'";
            return TransactionHelper.errorResponse(clientHandler, response, invokeFromExec);
        }

        if (TransactionHelper.isHandlingTransaction(clientHandler, args)) return null;

        String key = args[1], value = args[2];
        MortalValue mortalValue = new MortalValue<> (value);

        if(args.length == 5 && "PX".equalsIgnoreCase(args[3])) {
            try {
                int lifespan = Integer.parseInt(args[4]);
                mortalValue.setExpirationTime(System.currentTimeMillis() + lifespan);
            } catch (NumberFormatException e) {
                response = "ERR invalid PX value";
                return TransactionHelper.errorResponse(clientHandler, response, invokeFromExec);
            }
        }

        storageManager.put(key, mortalValue);
        response = RESPEncoder.encodeSimpleString("OK");
        if (!invokeFromExec) {
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
        }

        String command = RESPEncoder.encodeArray(
          new String[] {"SET", key, value}
        );
        ReplicaManager.propagateCommand(command);

        return response;
    }

}

