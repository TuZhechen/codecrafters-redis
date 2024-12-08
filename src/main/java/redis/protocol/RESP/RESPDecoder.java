package redis.protocol.RESP;

import redis.protocol.RESP.parsers.*;

import java.io.BufferedReader;
import java.io.IOException;


public class RESPDecoder {
    public static String decodeSimpleString(String respString) {
        return new SimpleStringImpl().decode(respString);
    }

    public static String decodeBulkString(String respString) {
        return new BulkStringImpl().decode(respString);
    }

    public static String[] decodeArray(String respString) {
        return new ArraysImpl().decode(respString);
    }

    public static String[] decodeCommand(BufferedReader reader) throws IOException {
        String head = reader.readLine();
        if (head == null) {
            throw new IOException("Invalid RESP command: empty or null input");
        }
        if (!head.startsWith("*")) {
            throw new IOException("Invalid RESP command: wrong head");
        }
        int argCount = Integer.parseInt(head.substring(1));
        String[] args = new String[argCount];
        for (int i = 0; i < argCount; i++) {
            String bulkLength = reader.readLine();
            if (bulkLength == null || !bulkLength.startsWith("$")) {
                throw new IOException("Invalid RESP command: wrong length");
            }

            String arg = reader.readLine();
            if (arg == null) {
                throw new IOException("Invalid RESP command: missing arg");
            }
            args[i] = arg;
        }
        return args;
    }

}
