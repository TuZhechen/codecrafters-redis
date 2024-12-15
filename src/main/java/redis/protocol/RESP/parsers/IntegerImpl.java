package redis.protocol.RESP.parsers;

public class IntegerImpl {
    public String encode(int rawValue) {
        return ":" + rawValue + "\r\n";
    }
    public int decode(String respString) {
        if (respString.startsWith(":")) {
            try {
                int res = Integer.parseInt(respString.substring(1, respString.length() - 2));
                return res;
            } catch (NumberFormatException e) {
                System.err.println("Nonnumeric RESP string");
            }
        }
        throw new IllegalArgumentException("Invalid RESP Integer: " + respString);
    }
}
