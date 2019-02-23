package de.hhu.bsinfo.dxgraphloader.formats.parsers;


import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public abstract class SimpleFormatReader extends GraphFormatReader {

    private BufferedReader reader;
    private ByteArrayInputStream byteStream;

    SimpleFormatReader(byte[] chunk) {
        byteStream = new ByteArrayInputStream(chunk);
        reader = new BufferedReader(new InputStreamReader(byteStream));
    }

    public SimpleFormatReader() {

    }

    public String readLine() throws IOException {
        return reader.readLine();
    }

    public boolean ready() throws IOException {
        return reader.ready();
    }

    public void close() throws IOException {
        if (byteStream != null) {
            byteStream.close();
        }
    }
}
