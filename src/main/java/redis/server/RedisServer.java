package redis.server;

import redis.core.RdbLoader;
import redis.core.ServerConfig;
import redis.core.StorageManager;
import redis.replication.ReplicaManager;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class RedisServer {
    private final ServerConfig serverConfig;
    private final StorageManager storageManager;

    public RedisServer(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
        this.storageManager = new StorageManager();
    }

    public void start() {
        int port = serverConfig.getPort();
        if (serverConfig.containsKey("dir") && serverConfig.containsKey("dbfilename")) {
            RdbLoader.loadRdbFile(serverConfig.getDir(),
                                  serverConfig.getDbFileName(),
                                  storageManager
                );
        }

        if (serverConfig.isMaster()) {
            startServer(port);
        } else {
            startReplicaServer(port);
        }
    }

    private void startServer(int port) {
        String role = serverConfig.get("role");
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            System.out.println(role + " server is running on port: " + port);

            while (true) {
                final Socket client = serverSocket.accept();
                new Thread(new ClientHandler(client, storageManager, serverConfig)).start();
            }
        } catch (IOException e) {
            System.err.println("Error starting" + role + " server: " + e.getMessage());
        }
    }

    private void startReplicaServer(int port) {
        System.out.println("Starting replica server...");

        // Use a CountDownLatch to synchronize replica initialization
        CountDownLatch replicaInitLatch = new CountDownLatch(1);

        // Start replica in a separate thread
        Thread replicaThread = new Thread(() -> {
            try {
                ReplicaManager.startReplica(serverConfig, storageManager);
            } catch (Exception e) {
                System.err.println("Replica initialization failed: " + e.getMessage());
            } finally {
                // Signal that replica initialization is complete
                replicaInitLatch.countDown();
            }
        });
        replicaThread.start();

        try {
            // Wait for replica to be fully initialized before starting server
            // Add a timeout to prevent indefinite waiting
            if (!replicaInitLatch.await(30, TimeUnit.SECONDS)) {
                System.err.println("Replica initialization timed out");
                return;
            }

            // Start server after replica is initialized
            startServer(port);

        } catch (InterruptedException e) {
            System.err.println("Replica server initialization interrupted: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

}
