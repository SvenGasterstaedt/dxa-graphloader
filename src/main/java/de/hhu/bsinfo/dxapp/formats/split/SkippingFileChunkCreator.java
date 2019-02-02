package de.hhu.bsinfo.dxapp.split;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class SkippingFileChunkCreator extends FileChunkCreator {

    protected byte[] content;
    protected long bytesTotal;
    protected RandomAccessFile randomAccessFile;

    protected int chunkSize;


    public SkippingFileChunkCreator(File file, int chunkSize) throws Exception {
        randomAccessFile = new RandomAccessFile(file, "r");
        bytesTotal = randomAccessFile.length();
        this.chunkSize = chunkSize;
        content = new byte[chunkSize];
    }


    public boolean hasRemaining() throws IOException {
        return (bytesTotal - randomAccessFile.getFilePointer()) > 0;
    }

    public long remaining() throws IOException {
        return (bytesTotal - randomAccessFile.getFilePointer());
    }

    public byte[] getNextChunk() throws IOException {
        if (hasRemaining()) {
            if (remaining() < chunkSize) {
                content = new byte[(int) remaining()]; //can be casted safely, because chunkSize is an Integer
                randomAccessFile.read(content);
                return content;
            }
            content = new byte[chunkSize];
            randomAccessFile.read(content);
            if (hasRemaining()) {
                byte[] remainingLine = randomAccessFile.readLine().concat("\n").getBytes();
                content = Arrays.copyOf(content, content.length + remainingLine.length);
                System.arraycopy(remainingLine, 0, content, content.length - remainingLine.length, remainingLine.length);
            }
            return content;


        }
        return null;
    }

}
