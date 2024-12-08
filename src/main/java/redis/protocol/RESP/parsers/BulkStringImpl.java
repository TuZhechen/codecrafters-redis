package redis.protocol.RESP.parsers;

public class BulkStringImpl {
    public String encode(String rawString) {
        int length = rawString.length();
        return "$" + length + "\r\n" + rawString + "\r\n";  // Format: $<length>\r\n<value>\r\n
    }

    public String encodeLength(int length) {
        return "$" + length + "\r\n";  // Format: $<length>\r\n for RDB files or large payloads
    }

    public String decode(String respString) {
        if (respString.startsWith("$")) {
            int length = Integer.parseInt(respString.substring(1, respString.indexOf('\r')));
            if (length == -1) {
                return null;  // Null Bulk String
            }
            return respString.substring(respString.indexOf('\n') + 1, respString.length() - 2);  // Extract value
        }
        throw new IllegalArgumentException("Invalid RESP Bulk String: " + respString);
    }
}
