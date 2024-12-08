package redis.server;

import redis.commands.RedisCommandFactory;
import redis.commands.RedisCommandHandler;
import redis.core.ServerConfig;
import redis.core.StorageManager;
import redis.protocol.RESP.RESPDecoder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private final StorageManager storageManager;
    private final ServerConfig serverConfig;
    private final BufferedReader reader;
    private final PrintWriter writer;

    public ClientHandler(Socket clientSocket, StorageManager storageManager, ServerConfig serverConfig) throws IOException {
        this.clientSocket = clientSocket;
        this.storageManager = storageManager;
        this.serverConfig = serverConfig;
        this.reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        this.writer = new PrintWriter(clientSocket.getOutputStream(), true);
    }

    @Override
    public void run() {
        try {
             while (true) {
                String[] args = RESPDecoder.decodeCommand(reader);
                System.out.println("Processing command: " + args[0]);
                if (args == null || args.length == 0) {
                    System.err.println("Invalid command received");
                    continue;
                }
                RedisCommandFactory redisCommandFactory = new RedisCommandFactory(storageManager, serverConfig);
                RedisCommandHandler handler = redisCommandFactory.getCommandHandler(args[0]);
                handler.invoke(args, this);
            }
        } catch (Exception e) {
            System.err.println("Error handling client commands: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
        }
    }

    public BufferedReader getReader() {
        return reader;
    }

    public PrintWriter getWriter() {
        return writer;
    }

    public Socket getClientSocket() {
        return clientSocket;
    }
}
