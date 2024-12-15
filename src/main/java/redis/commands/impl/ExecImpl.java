package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.server.ClientHandler;

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

        response = "-ERR EXEC without MULTI\r\n";
        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();
    }
}
