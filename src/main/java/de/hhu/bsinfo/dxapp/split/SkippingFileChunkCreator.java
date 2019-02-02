package de.hhu.bsinfo.dxapp.split;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class SkippingFileChunkCreator {

    protected byte[] content;
    protected long left_to_read;
    protected RandomAccessFile randomAccessFile;

    protected int approximatedChunkSize;

    public SkippingFileChunkCreator(File file) throws Exception {
        randomAccessFile = new RandomAccessFile(file, "r");
        left_to_read = randomAccessFile.length();
        approximatedChunkSize = (int) (left_to_read);
        content = new byte[approximatedChunkSize];
    }

    public SkippingFileChunkCreator(File file, int chunk_size) throws Exception {
        randomAccessFile = new RandomAccessFile(file, "r");
        left_to_read = randomAccessFile.length();
        approximatedChunkSize = chunk_size;
        content = new byte[approximatedChunkSize];
    }

    public boolean ready() {
        return left_to_read > 0;
    }

    public byte[] getNextChunk() throws IOException {
        if (left_to_read <= approximatedChunkSize) {
            content = new byte[(int) left_to_read];
            left_to_read -= randomAccessFile.read(content);
            return content;
        }else {
            content = new byte[approximatedChunkSize];
            left_to_read -= randomAccessFile.read(content);
            if (ready()) {
                byte[] remainingBytes = randomAccessFile.readLine().concat("\n").getBytes();
                content = Arrays.copyOf(content, content.length + remainingBytes.length);
                System.arraycopy(remainingBytes, 0, content, content.length - remainingBytes.length, remainingBytes.length);
                left_to_read -= remainingBytes.length;
            }
            return content;
        }
    }
}
