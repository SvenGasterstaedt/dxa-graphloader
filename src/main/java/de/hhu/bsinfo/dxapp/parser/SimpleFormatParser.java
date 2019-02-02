package de.hhu.bsinfo.dxapp.parser;


import de.hhu.bsinfo.dxapp.data.Edge;
import de.hhu.bsinfo.dxapp.data.Vertex;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public abstract class SimpleFormatParser extends GraphFormatParser {

    private BufferedReader reader;
    private ByteArrayInputStream byteStream;

    SimpleFormatParser(byte[] chunk) {
        super(chunk);
        byteStream = new ByteArrayInputStream(chunk);
        reader = new BufferedReader(new InputStreamReader(byteStream));
    }

    public String readLine() throws IOException {
            return reader.readLine();
    }

    @Override
    public boolean ready() throws IOException {
        return reader.ready();
    }

    public void close() throws IOException {
        if(byteStream!=null){
            byteStream.close();
        }
    }
}
