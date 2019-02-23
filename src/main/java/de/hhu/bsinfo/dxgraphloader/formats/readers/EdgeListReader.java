package de.hhu.bsinfo.dxgraphloader.formats.readers;

import de.hhu.bsinfo.dxgraphloader.app.data.PeerVertexMap;
import de.hhu.bsinfo.dxgraphloader.app.data.formats.SimpleFormatReader;
import de.hhu.bsinfo.dxgraphloader.graph.data.Edge;
import de.hhu.bsinfo.dxgraphloader.graph.data.Vertex;
import de.hhu.bsinfo.dxram.chunk.ChunkService;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class EdgeListReader extends SimpleFormatReader {
    @Override
    public boolean execute(byte[] content, final ChunkService chunkService, final short current_peer, PeerVertexMap peerVertexMap) {
        Map<String, Vertex> vertexMap = new HashMap<String, Vertex>();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(content)));
        try {

            while (bufferedReader.ready()) {
                String line = bufferedReader.readLine();
                if (line.charAt(0) == '#') continue;
                int index = line.indexOf('\t');
                String key1 = line.substring(0, index);
                String key2 = line.substring(index + 1);
                Vertex vertex1 = keyToVertex(chunkService, current_peer, vertexMap, key1);
                Vertex vertex2 = keyToVertex(chunkService, current_peer, vertexMap, key2);

                Edge edge = new Edge();
                chunkService.create().create(current_peer, edge);
                edge.setEndPoint(vertex1, vertex2);
                chunkService.put().put(edge);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}
