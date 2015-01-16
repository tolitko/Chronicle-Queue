package net.openhft.chronicle.tcp;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.tools.WrappedExcerptAppender;
import net.openhft.lang.io.ByteBufferBytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Created by Rob Austin
 */
public class StatelessAppender extends WrappedExcerptAppender {

    private ByteBufferBytesAppender appender;
    private int capacity;

    private static class ByteBufferBytesAppender extends ByteBufferBytes implements ExcerptAppender {

        public ByteBufferBytesAppender(ByteBuffer buffer) {
            super(buffer);
        }

        @Override
        public void startExcerpt() {
            buffer().clear();
        }

        @Override
        public void startExcerpt(long capacity) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean wasPadding() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long index() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long lastWrittenIndex() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExcerptAppender toEnd() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Chronicle chronicle() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addPaddedEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean nextSynchronous() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void nextSynchronous(boolean nextSynchronous) {
            throw new UnsupportedOperationException();
        }
    }


    public StatelessAppender(int defaultCapacity) {
        super(new ByteBufferBytesAppender(ByteBuffer.allocate(defaultCapacity)));
        capacity = defaultCapacity;
    }

    @Override
    public void startExcerpt(long capacity) {

        if (capacity > Integer.MAX_VALUE)
            throw new IllegalArgumentException("Only capacities up to Integer.MAX_VALUE are supported");

        if (capacity > this.capacity) {
            appender = new ByteBufferBytesAppender(ByteBuffer.allocate((int) capacity));
            this.capacity = (int) capacity;
        }

        super.startExcerpt();
    }

    private SocketChannel socketChannel;

    @Override
    public boolean isFinished() {
        try {

            if (socketChannel == null)
                socketChannel = openSocketChannel();

            appender.flip();

            while (appender.remaining() > 0) {
                int sent = socketChannel.write(appender.buffer());
                if (sent == -1)
                    return false;
            }

            appender.clear();

            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    static SocketChannel openSocketChannel() throws IOException {
        SocketChannel result = SocketChannel.open();
        result.socket().setTcpNoDelay(true);

        return result;
    }
}
