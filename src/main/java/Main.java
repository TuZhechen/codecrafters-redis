import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static final Map<String, String> config = new ConcurrentHashMap<>();
    public static final Map<String, MortalValue> store = new ConcurrentHashMap<>();

  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");
    // Parse command-line arguments
      for (int i = 0; i < args.length; ++i) {
          if (args[i].equals("--dir") && i+1 < args.length) {
              config.put("dir", args[i+1]);
          } else if (args[i].equals("--dbfilename") && i+1 < args.length) {
              config.put("dbfilename", args[i+1]);
          }
      }
      if(!config.isEmpty()) {
        RdbLoader.loadRdbFile(config.get("dir"), config.get("dbfilename"), store);
      }
    ExecutorService threadPool = Executors.newCachedThreadPool();
    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = 6379;
    try {
        serverSocket = new ServerSocket(port);
        // Since the tester restarts your program quite often, setting SO_REUSEADDR
        // ensures that we don't run into 'Address already in use' errors
        serverSocket.setReuseAddress(true);
        // Wait for connection from client.
        while(true){
            clientSocket = serverSocket.accept();
            final Socket client = clientSocket;
            ClientHandler clientHandler = new ClientHandler(client, config, store);
            Thread clientThread = new Thread(clientHandler);
            clientThread.start();
        }
    } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
    } finally {
        try {
            if (serverSocket != null) {
              serverSocket.close();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
  }

}
