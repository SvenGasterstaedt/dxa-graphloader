package de.hhu.bsinfo.dxgraphloader.formats.splitter;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.loader.data.FileChunk;
import de.hhu.bsinfo.dxgraphloader.loader.formats.FileChunkCreator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class SkippingLineFileChunkCreator extends FileChunkCreator {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());

    protected byte[] content;
    protected long bytesTotal;
    protected RandomAccessFile randomAccessFile;

    protected int chunkSize;


    public SkippingLineFileChunkCreator(String file, int chunkSize) {
        try {
            randomAccessFile = new RandomAccessFile(file, "r");
            bytesTotal = randomAccessFile.length();
            this.chunkSize = chunkSize;
            content = new byte[chunkSize];
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("IOException in FileChunkCreator");
        }
    }

    @Override
    public boolean hasRemaining() {
        try {
            return (bytesTotal - randomAccessFile.getFilePointer()) > 0;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public long remaining() throws IOException {
        return (bytesTotal - randomAccessFile.getFilePointer());
    }


    @Override
    public FileChunk getNextChunk() {
        try {
            if (hasRemaining()) {
                if (remaining() < chunkSize) {
                    content = new byte[(int) remaining()]; //can be casted safely, because chunkSize is an Integer
                    randomAccessFile.read(content);
                    return new FileChunk(content);
                }

                content = new byte[chunkSize];
                randomAccessFile.read(content);

                byte[] remainingLine = new byte[0];
                if (hasRemaining()) {
                    remainingLine = randomAccessFile.readLine().concat("\n").getBytes();
                }
                content = ByteBuffer.allocate(content.length + remainingLine.length).put(content).put(remainingLine).array();
                return new FileChunk(content);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    @Override
    public int getApproxChunkAmount() {
        return (int) (bytesTotal / chunkSize + 2);
    }

    @Override
    public void setCycle(short cycle) {

    }

}
