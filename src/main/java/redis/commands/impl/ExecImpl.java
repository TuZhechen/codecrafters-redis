package redis.commands.impl;

import redis.commands.RedisCommandFactory;
import redis.commands.RedisCommandHandler;
import redis.commands.TransactionHelper;
import redis.core.ServerConfig;
import redis.core.StorageManager;
import redis.protocol.RESP.RESPDecoder;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class ExecImpl implements RedisCommandHandler {
    private final StorageManager storageManager;
    private final ServerConfig serverConfig;

    public ExecImpl(StorageManager storageManager, ServerConfig serverConfig) {
        this.storageManager = storageManager;
        this.serverConfig = serverConfig;
    }

    @Override
    public String invoke(String[] args, ClientHandler clientHandler, boolean invokeFromExec) {
        String response;
        if (args.length > 1) {
            response = "ERR wrong number of arguments for 'EXEC'";
            return TransactionHelper.errorResponse(clientHandler, response, invokeFromExec);
        }

        if (!clientHandler.isInTransaction()) {
            response = "ERR EXEC without MULTI";
            return TransactionHelper.errorResponse(clientHandler, response, invokeFromExec);
        }

        Queue<String []> commandQueue = clientHandler.getCommandQueue();
        if (commandQueue.isEmpty()) {
            response = RESPEncoder.encodeArray(new Object[] {});
            clientHandler.setInTransaction(false);
        } else {
            clientHandler.setInTransaction(false);
            RedisCommandFactory factory = new RedisCommandFactory(storageManager, serverConfig);
            List<Object> responses = new ArrayList<>();
            while (!commandQueue.isEmpty()) {
                String[] command = commandQueue.poll();
                try {
                    RedisCommandHandler handler = factory.getCommandHandler(command[0].toUpperCase());
                    String res = handler.invoke(command, clientHandler, true);
                    switch(res.charAt(0)) {
                        case '+':
                            responses.add(RESPDecoder.decodeSimpleString(res));
                            break;
                        case '$':
                            responses.add(RESPDecoder.decodeBulkString(res));
                            break;
                        case '*':
                            responses.add(RESPDecoder.decodeArray(res));
                            break;
                        case ':':
                            responses.add(RESPDecoder.decodeInteger(res));
                            break;
                        case '-':
                            responses.add(RESPDecoder.decodeErrorString(res));
                            break;
                        default:
                            break;
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            response = RESPEncoder.encodeArray(responses.toArray());
        }

        if (!invokeFromExec) {
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
        }
        return response;
    }
}
