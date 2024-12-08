package redis.commands.impl;

import redis.commands.RedisCommandHandler;
import redis.protocol.RESP.RESPEncoder;
import redis.replication.ReplicaManager;
import redis.server.ClientHandler;

public class WaitImpl implements RedisCommandHandler {
    @Override
    public void invoke(String[] args, ClientHandler clientHandler) {
        String response;
        int numOfReplicas = ReplicaManager.numOfReplicas;
        if (args.length != 3) {
            response = "-ERR wrong number of arguments for 'WAIT'\r\n";
            clientHandler.getWriter().print(response);
            clientHandler.getWriter().flush();
            return;
        }
        response = RESPEncoder.encodeInteger(numOfReplicas);
        clientHandler.getWriter().print(response);
        clientHandler.getWriter().flush();
    }
}
