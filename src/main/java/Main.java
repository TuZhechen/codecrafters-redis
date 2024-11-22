import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

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
            threadPool.submit(() -> handleClient(client));
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

  private static void handleClient(Socket clientSocket) {
      try {
          InputStream inputStream = clientSocket.getInputStream();
          OutputStream outputStream = clientSocket.getOutputStream();
          BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
          String line;
          StringBuilder commandBuffer = new StringBuilder();
          while((line = reader.readLine()) != null) {
              commandBuffer.append(line);
              if (commandBuffer.toString().contains("PING")) {
                outputStream.write("+PONG\r\n".getBytes());
                commandBuffer.setLength(0);
              }
          }
      } catch (IOException e) {
          System.out.println("IOException when handling client: " + e.getMessage());
      } finally {
          try {
              clientSocket.close();
          } catch (IOException e) {
              System.out.println("IOException when closing client " + e.getMessage());
          }
      }
  }
}
