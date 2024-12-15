package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

import java.util.Queue;

public class ExecImpl implements RedisCommandHandler {

    @Override
    public void invoke(String[] args, ClientHandler clientHandler) {
        String response;
        if (args.length > 1) {
            response = "-ERR wrong number of arguments for 'EXEC'\r\n";
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
            return;
        }

        if (!clientHandler.isInTransaction()) {
            response = "-ERR EXEC without MULTI\r\n";
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
            return;
        }

        Queue<String []> commandQueue = clientHandler.getCommandQueue();
        if (commandQueue.isEmpty()) {
            response = RESPEncoder.encodeArray(new Object[] {});
            clientHandler.setInTransaction(false);
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
            return;
        }
    }
}
