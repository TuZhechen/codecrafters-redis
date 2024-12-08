package redis.protocol.RESP.parsers;

public class ArraysImpl {
    public String encode(String[] elements) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(elements.length).append("\r\n");  // Format: *<num-elements>\r\n
        for (String element : elements) {
            sb.append(new BulkStringImpl().encode(element));  // Each element as a Bulk String
        }
        return sb.toString();
    }

    public String[] decode(String respString) {
        if (respString.startsWith("*")) {
            int numElements = Integer.parseInt(respString.substring(1, respString.indexOf('\r')));
            String[] elements = new String[numElements];
            String[] lines = respString.split("\r\n");
            int lineIndex = 2;
            for (int i = 0; i < numElements; i++) {
                elements[i] = lines[lineIndex];
                lineIndex += 2;
            }
            return elements;
        }
        throw new IllegalArgumentException("Invalid RESP Array: " + respString);
    }
}

