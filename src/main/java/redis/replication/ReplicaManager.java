package redis.replication;

import redis.core.ServerConfig;
import redis.core.StorageManager;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicaManager {

    // Map to hold replica buffers, one for each replica identified by its socket
    public static final Map<Socket, BlockingQueue<String>> replicaBuffers = new ConcurrentHashMap<>();

    public static void addReplica(Socket replicaSocket, BlockingQueue<String> replicaBuffer) {
        replicaBuffers.put(replicaSocket, replicaBuffer);
    }

    // Method to propagate a command to all replicas
    public static void propagateCommand(String command) {
        // Send the command to all connected replicas
        for (BlockingQueue<String> buffer : replicaBuffers.values()) {
            buffer.offer(command);
        }
    }


    public static void startCommandPropagator(Socket replicaSocket, BlockingQueue<String> replicaBuffer) {
        new Thread(() -> {
            try (OutputStream replicaOutput = replicaSocket.getOutputStream()){
                while (!replicaSocket.isClosed()) {
                    String command = replicaBuffer.take();
                    replicaOutput.write(command.getBytes());
                    replicaOutput.flush();
                }
            } catch (Exception e) {
                System.err.println("Error propagating commands to replica: " + e.getMessage());
            } finally {
                replicaBuffers.remove(replicaSocket);
                System.out.println("Replica disconnected: " +
                        replicaSocket.getInetAddress());
            }
        }).start();
    }

    public static void startReplica(ServerConfig serverConfig, StorageManager storageManager) {
        String masterHost = serverConfig.getMasterHost();
        int masterPort = serverConfig.getMasterPort();
        try {
            Socket masterSocket = new Socket(masterHost, masterPort);
            System.out.println("Connected to master at: " + masterHost + ":" + masterPort);
            ReplicaHandler replicaHandler = new ReplicaHandler(serverConfig, storageManager, masterSocket);
            replicaHandler.start();
         } catch (IOException e) {
            System.err.println("Error starting replica: " + e.getMessage());
        }
    }

    public static int getReplicaCount() {
        return replicaBuffers.size();
    }

}
