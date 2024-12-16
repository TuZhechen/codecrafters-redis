package redis.protocol.RESP.parsers;

import java.util.List;

public class ArraysImpl {
    public String encode(Object[] elements) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(elements.length).append("\r\n");  // Format: *<num-elements>\r\n
        for (Object element : elements) {
            if (element instanceof String) {
                if (((String) element).startsWith("ERR")) {
                    sb.append(new ErrorStringImpl().encode((String)element));
                } else {
                    sb.append(new BulkStringImpl().encode((String) element));
                }
            } else if (element instanceof Integer) {
                sb.append(new IntegerImpl().encode((Integer) element));
            } else if (element instanceof Object[]) {
                sb.append((encode((Object[]) element)));
            } else if (element instanceof List) {
                sb.append(encode(((List<?>) element).toArray()));
            } else {
                sb.append(new BulkStringImpl().encode(element.toString()));
            }
        }
        return sb.toString();
    }

    private static class DecodeResult {
        Object[] array;
        Object value;
        int nextIndex;

        DecodeResult(Object[] array, int nextIndex) {
            this.array = array;
            this.nextIndex = nextIndex;
        }

        DecodeResult(Object value, int nextIndex) {
            this.value = value;
            this.nextIndex = nextIndex;
        }
    }

    public Object[] decode(String respString) {
        if (!respString.startsWith("*")) {
            throw new IllegalArgumentException("Invalid RESP Array: " + respString);
        }

        DecodeResult result = decodeArray(respString,0);
        return result.array;
    }

    private DecodeResult decodeArray(String respString, int startIndex) {
        int newlineIndex = respString.indexOf('\r');
        int numElements = Integer.parseInt(respString.substring(startIndex + 1, newlineIndex));

        Object[] elements = new String[numElements];
        int currentIndex = newlineIndex + 2;

        for (int i = 0; i < numElements; i++) {
            char firstChar = respString.charAt(currentIndex);
            switch (firstChar) {
                case '$':
                    DecodeResult stringResult = decodeBulkString(respString, currentIndex);
                    elements[i] = stringResult.value;
                    currentIndex = stringResult.nextIndex;
                    break;
                case '*':
                    DecodeResult arrayResult = decodeArray(respString, currentIndex);
                    elements[i] = arrayResult.array;
                    currentIndex = arrayResult.nextIndex;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported element at index: " + currentIndex);
            }
        }

        return new DecodeResult(elements, currentIndex);
    }

    private DecodeResult decodeBulkString(String respString, int startIndex) {
        int lengthEnd = respString.indexOf('\r', startIndex);
        int length = Integer.parseInt(respString.substring(startIndex+1, lengthEnd));

        if (length == -1) {
            return new DecodeResult(null, lengthEnd + 2);
        }

        int valueStart = lengthEnd + 2;
        String value = respString.substring(valueStart, valueStart + length);

        int nextIndex = valueStart + length + 2;
        return new DecodeResult(value, nextIndex);
    }
}

