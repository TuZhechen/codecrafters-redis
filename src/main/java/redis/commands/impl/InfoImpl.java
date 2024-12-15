package redis.commands.impl;

import redis.core.ServerConfig;
import redis.commands.RedisCommandHandler;
import redis.commands.TransactionHelper;
import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

public class InfoImpl implements RedisCommandHandler{
    private final ServerConfig serverConfig;

    public InfoImpl(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    @Override
    public void invoke(String[] args, ClientHandler clientHandler) {
        if (TransactionHelper.isHandlingTransaction(clientHandler, args)) return;
        String response, info;
        if ("slave".equalsIgnoreCase(serverConfig.get("role"))) {
            info = "role:slave";
        } else {
            String replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
            info = "role:master\n" +
                    "master_replid:" + replid + "\n" +
                    "master_repl_offset:0";
        }
        response = RESPEncoder.encodeBulkString(info);
        clientHandler.getWriter().write(response);
        clientHandler.getWriter().flush();
    }
}
