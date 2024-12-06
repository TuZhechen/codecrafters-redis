import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.*;

public class Main {
    public static final Map<String, String> config = new ConcurrentHashMap<>();
    public static final Map<String, MortalValue> store = new ConcurrentHashMap<>();
    public static final Map<Socket, BlockingQueue<String>> replicaBuffers = new ConcurrentHashMap<>();

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
                  System.out.println("C-S connection closed");
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
          readMasterResponse(inputStream);
          // handshake 3/3
          sendPsync(outputStream);
          readMasterResponse(inputStream);
          // skip rdb file transfer
          skipRdbFile(inputStream);
          new Thread(() -> listenForCommands(inputStream, outputStream)).start();
          startServer();
      } catch (IOException e) {
          System.err.println("IOException in replica" + e.getMessage());
      }
    }

    public static void startCommandPropagator(Socket replicaSocket, BlockingQueue<String> buffer) {
      new Thread( () -> {
              try (OutputStream replicaOutput = replicaSocket.getOutputStream()) {
                  while (!replicaSocket.isClosed()) {
                      try {
                          String cmd = buffer.take();
                          replicaOutput.write(cmd.getBytes());
                          replicaOutput.flush();
                      } catch (IOException e) {
                          System.err.println("Error propagating to replica: " + e.getMessage());
                          break;
                      }
                  }
              } catch (IOException | InterruptedException e) {
                  Thread.currentThread().interrupt();
                  System.err.println("Command propagation interrupted");
              } finally {
                  replicaBuffers.remove(replicaSocket);
                  System.out.println("Replica disconnected: " +
                          replicaSocket.getInetAddress());
              }
        }).start();
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

    private static void sendPsync(OutputStream outputStream) throws IOException {
      System.out.println("Sending PSYNC to master...");
      String request = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
      outputStream.write(request.getBytes());
    }

    private static void readMasterResponse(InputStream inputStream) throws IOException {
//      BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
//      String response = reader.readLine();
//      System.out.println("Received response from master: " + response);
        byte[] buffer = new byte[56];
        int bytesRead = inputStream.read(buffer);

        if (bytesRead > 0) {
            String response = new String(buffer, 0 , bytesRead);
            System.out.println("Received response from master: " + response);
            System.out.println("Bytes read: " + bytesRead);
        } else {
            System.out.println("No bytes read from input stream");
        }
    }

    private static void skipRdbFile(InputStream inputStream) throws IOException {
        // Read length prefix
        byte[] lengthBuffer = new byte[5];
        inputStream.read(lengthBuffer);
        String lengthPrefix = new String(lengthBuffer);

        if (!lengthPrefix.startsWith("$")) {
            System.out.println("Unexpected RDB length prefix: " + lengthPrefix);
            return;
        }

        // Parse RDB file length
        int rdbLength = Integer.parseInt(lengthPrefix.substring(1, lengthPrefix.indexOf('\r')));

        // Skip RDB content
        byte[] rdbBuffer = new byte[rdbLength];
        long skipped = inputStream.read(rdbBuffer);

        System.out.println("Skipped RDB file:");
        System.out.println("Length prefix: " + lengthPrefix);
        System.out.println("RDB file length: " + rdbLength);
        System.out.println("Bytes skipped: " + skipped);
    }

    private static void listenForCommands(InputStream inputStream, OutputStream outputStream) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        try {
            while (true) {
                String[] args = ClientHandler.parseRespCommand(reader);
                if (args == null || args.length == 0) {
                    System.err.println("No commands received from master.");
                    break;
                }

                String command = args[0].toUpperCase();
                if ("REPLCONF".equals(command)) {
                    handleReplConfCommand(args, outputStream);
                } else {
                    handlePropagatedCommand(args);
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading commands: " + e.getMessage());
        }
    }

    private static void handlePropagatedCommand(String[] args) {
        String command = args[0].toUpperCase();
        switch (command) {
            case "SET":
                if (args.length < 3) {
                    System.err.println("Invalid SET from master, too few arguments");
                    return;
                }
                String key = args[1], value = args[2];
                synchronized (store) {
                    store.put(key, new MortalValue(value));
                }
                break;
                default:
                    break;
              }

    }

    private static void handleReplConfCommand(String[] args, OutputStream outputStream) throws IOException {
        if ("GETACK".equalsIgnoreCase(args[1])) {
            String resp = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n";
            outputStream.write(resp.getBytes());
            outputStream.flush();
            System.out.println("Sent REPLCONF ACK 0 to master.");
        } else {
            System.out.println("Unhandled REPLCONF command: " + args[1]);
        }
    }

}
