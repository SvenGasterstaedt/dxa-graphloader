package de.hhu.bsinfo.dxgraphloader.loader.formats;


import de.hhu.bsinfo.dxgraphloader.graph.data.Vertex;
import de.hhu.bsinfo.dxram.chunk.ChunkService;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

public abstract class SimpleFormatReader extends GraphFormatReader {

    private BufferedReader reader;
    private ByteArrayInputStream byteStream;

    public SimpleFormatReader(byte[] chunk) {
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

    public Vertex keyToVertex(ChunkService chunkService, short current_peer, Map<String, Vertex> vertexMap, String key1) {
        Vertex vertex1;
        if (vertexMap.containsKey(key1)) {
            return vertexMap.get(key1);
        } else {
            vertex1 = new Vertex();
            vertexMap.put(key1, vertex1);
            chunkService.create().create(current_peer, vertex1);
            return vertex1;
        }
    }
}
