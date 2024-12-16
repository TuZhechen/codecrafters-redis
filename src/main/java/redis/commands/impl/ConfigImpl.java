package redis.commands.impl;

import redis.commands.TransactionHelper;
import redis.core.ServerConfig;
import redis.commands.RedisCommandHandler;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

public class ConfigImpl implements RedisCommandHandler {
    private final ServerConfig serverConfig;

    public ConfigImpl(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    @Override
    public String invoke(String[] args, ClientHandler clientHandler, boolean invokeFromExec) {
        String response;
        if (args.length < 3) {
            response = RESPEncoder.encodeErrorString("ERR wrong number of arguments for 'CONFIG'");
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
            return null;
        }

        if (TransactionHelper.isHandlingTransaction(clientHandler, args)) return null;

        String option = args[1], name = args[2];
        if ("GET".equalsIgnoreCase(option)) {
            String value = serverConfig.get(name);
            response = RESPEncoder.encodeArray(
              new String[] {name, value}
            );
            if (!invokeFromExec) {
                clientHandler.getWriter().print(response);
                clientHandler.getWriter().flush();
            }
            return response;
        }

        return null;
    }
}
