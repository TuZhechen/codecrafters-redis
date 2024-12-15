package redis.commands;

import redis.protocol.RESP.RESPEncoder;
import redis.server.ClientHandler;

public class TransactionHelper {
    public static boolean isHandlingTransaction(ClientHandler handler, String[] args) {
        if (handler.isInTransaction()) {
            handler.getCommandQueue().add(args);
            String response = RESPEncoder.encodeSimpleString("QUEUED");
            handler.getWriter().print(response);
            handler.getWriter().flush();
            return true;
        }
        return false;
    }
}
