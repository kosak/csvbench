package io.deephaven;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public final class RepeatedStream extends InputStream {
    private final byte[] body;
    private int bodyCount;
    private byte[] data;
    private int current;
    private final byte[] bufferForRead;

    public RepeatedStream(final String prefix, final String body, int bodyCount) {
        this.body = body.getBytes(StandardCharsets.UTF_8);
        this.bodyCount = bodyCount;
        data = prefix.getBytes(StandardCharsets.UTF_8);
        current = 0;
        bufferForRead = new byte[1];
    }

    @Override
    public int read() {
        final int bytesRead = read(bufferForRead, 0, 1);
        if (bytesRead < 0) {
            return bytesRead;
        }
        return bufferForRead[0] & 0xff;

    }

    @Override
    public int read(byte[] b, int off, int len) {
        if (len == 0) {
            return 0;
        }
        // This is a 'while' to handle the edge case (doesn't happen in normal practice) where body.length == 0
        while (current == data.length) {
            if (bodyCount == 0) {
                return -1;
            }
            data = body;
            current = 0;
            --bodyCount;
        }

        final int remaining = data.length - current;
        final int amountToCopy = Math.min(len, remaining);
        System.arraycopy(data, current, b, off, amountToCopy);
        current += amountToCopy;
        return amountToCopy;
    }
}
