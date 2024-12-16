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

    public static String errorResponse(ClientHandler handler, String message, boolean exec) {
        String response;
        response = RESPEncoder.encodeErrorString(message);
        if (!exec) {
            handler.getWriter().print(response);
            handler.getWriter().flush();
        }
        return response;
    }
}
