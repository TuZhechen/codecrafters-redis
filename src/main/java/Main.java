import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
    public static final Map<String, String> config = new ConcurrentHashMap<>();
    public static final Map<String, MortalValue> store = new ConcurrentHashMap<>();

  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");
    // Parse command-line arguments
      String masterHost = null;
      int masterPort = -1;
      for (int i = 0; i < args.length - 1; ++i) {
          String arg = args[i], value = args[i+1];
          switch (arg) {
              case "--dir":
                  config.put("dir", value);
                  break;
              case "--dbfilename":
                  config.put("dbfilename", value);
                  break;
              case "--port":
                  config.put("port", value);
                  break;
              case "--replicaof":
                  String[] masterInfo = value.split(" ");
                  if (masterInfo.length == 2) {
                      masterHost = masterInfo[0];
                      masterPort = Integer.parseInt(masterInfo[1]);
                      config.put("role", "slave");
                      config.put("master_post", masterHost);
                      config.put("master_port", String.valueOf(masterPort));
                  }
                  break;
              default:
                  break;
          }
      }
      config.putIfAbsent("role", "master");
      if(config.containsKey("dir") && config.containsKey("dbfilename")) {
        RdbLoader.loadRdbFile(config.get("dir"), config.get("dbfilename"), store);
      }
      if("slave".equalsIgnoreCase(config.get("role"))) {
          startReplica(masterHost, masterPort);
      } else {
          startServer();
      }
  }

    private static void startServer() {
        ServerSocket serverSocket = null;
        int port = config.containsKey("port") ? Integer.parseInt(config.get("port")): 6379;
        try {
            System.out.println(config.get("role") + " server is running on port: " + port);
            serverSocket = new ServerSocket(port);
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
            // Wait for connection from client.
            while (true) {
                Socket clientSocket = serverSocket.accept();
                final Socket client = clientSocket;
                ClientHandler clientHandler = new ClientHandler(client, config, store);
                Thread clientThread = new Thread(clientHandler);
                clientThread.start();
            }
        } catch (IOException e) {
            System.out.println("Error setting C-S connection: " + e.getMessage());
        } finally {
            try {
                if (serverSocket != null) {
                  serverSocket.close();
                }
            } catch (IOException e) {
                System.out.println("Error closing server socket: " + e.getMessage());
            }
        }
    }

    private static void startReplica(String masterHost, int masterPort) {
      try (Socket masterSocket = new Socket(masterHost, masterPort)) {
          System.out.println("Connected to master at: " + masterHost + ":" + masterPort);
          // send a PING to the master
          OutputStream outputStream = masterSocket.getOutputStream();
          InputStream inputStream = masterSocket.getInputStream();
          // handshake 1/3
          sendPing(outputStream);
          readMasterResponse(inputStream);
          // handshake 2/3
          sendReplConf(outputStream, config.get("port"));
          readMasterResponse(inputStream);
          // allow clients to connect
          startServer();
      } catch (IOException e) {
          System.out.println("IOException in replica" + e.getMessage());
      }
    }

    private static void sendPing(OutputStream outputStream) throws IOException {
      System.out.println("Sending PING to master...");
      String pingRequest = "*1\r\n$4\r\nPING\r\n";
      outputStream.write(pingRequest.getBytes());
    }

    private static void sendReplConf(OutputStream outputStream, String port) throws IOException {
      System.out.println("Sending REPL config to master...");
      String firstRequest = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" + port + "\r\n";
      outputStream.write(firstRequest.getBytes());
      String secondRequest = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
      outputStream.write(secondRequest.getBytes());
    }

    private static void readMasterResponse(InputStream inputStream) throws IOException {
      BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
      String response = reader.readLine();
      System.out.println("Received response from master: " + response);
    }

}
