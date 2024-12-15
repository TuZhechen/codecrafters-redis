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
    public void invoke(String[] args, ClientHandler clientHandler) {
        String response;
        if (args.length < 3) {
            response = "-ERR wrong number of arguments for 'CONFIG'\r\n";
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
            return;
        }

        if (TransactionHelper.isHandlingTransaction(clientHandler, args)) return;

        String option = args[1], name = args[2];
        if ("GET".equalsIgnoreCase(option)) {
            String value = serverConfig.get(name);
            response = RESPEncoder.encodeArray(
              new String[] {name, value}
            );
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
        }
    }
}
