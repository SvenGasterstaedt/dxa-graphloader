package de.hhu.bsinfo.dxgraphloader.formats.splitter;

import de.hhu.bsinfo.dxgraphloader.app.data.FileChunk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NeighborLineSplitter extends SkippingLineSplitter {

    private final static Pattern p = Pattern.compile("\r\n|\r|\n");

    private long lineNumber = 0;

    public NeighborLineSplitter(String file, int chunkSize) throws Exception {
        super(file, chunkSize);
    }


    @Override
    public FileChunk getNextChunk() {
        try {
            if (hasRemaining()) {
                if (remaining() < chunkSize) {
                    content = new byte[(int) remaining()]; //can be casted safely, because chunkSize is an Integer
                    randomAccessFile.read(content);

                    content = ByteBuffer.allocate(Long.BYTES + content.length).putLong(lineNumber).put(content).array();
                    Matcher m = p.matcher(new String(content));
                    while (m.find()) {
                        lineNumber++;
                    }
                    return new FileChunk(content);
                }

                content = new byte[chunkSize];
                randomAccessFile.read(content);

                byte[] remainingLine = new byte[0];
                if (hasRemaining()) {
                    remainingLine = randomAccessFile.readLine().concat("\n").getBytes();
                }
                content = ByteBuffer.allocate(Long.BYTES + content.length + remainingLine.length).putLong(lineNumber).put(content).put(remainingLine).array();
                Matcher m = p.matcher(new String(content));
                while (m.find()) {
                    lineNumber++;
                }
                return new FileChunk(content);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }
    /*
    @Override
    public FileChunk getNextChunk() {
        StringBuilder builder = new StringBuilder();
        try {
            while (builder.length() * Character.BYTES < chunkSize && hasRemaining()) {
                builder.append(lineNumber).append("\t").append(randomAccessFile.readLine().concat("\n"));
                lineNumber++;
            }
            return new FileChunk(builder.toString().getBytes());
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }*/

    @Override
    public int getApproxChunkAmount() {
        return (int) (bytesTotal / chunkSize + 2);
    }
}
