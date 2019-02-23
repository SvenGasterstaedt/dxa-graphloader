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

@SuppressWarnings("Duplicates")
public class NeighborListReader extends SimpleFormatReader {

    @Override
    public boolean execute(byte[] content, final ChunkService chunkService, final short current_peer, PeerVertexMap peerVertexMap) {
        Map<String, Vertex> vertexMap = new HashMap<String, Vertex>();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(content)));
        try {

            while (bufferedReader.ready()) {
                String line = bufferedReader.readLine();
                if (line.charAt(0) == '#') continue;

                String[] key = line.split("\t");
                Vertex vertex1 = keyToVertex(chunkService,current_peer,vertexMap,key[0]);

                for (int i = 1; i < key.length; i++) {
                    Vertex vertex2 = keyToVertex(chunkService,current_peer,vertexMap,key[0]);
                    Edge edge = new Edge();
                    chunkService.create().create(current_peer, edge);
                    edge.setEndPoint(vertex1,vertex2);
                    chunkService.put().put(edge);
                }
            }
        } catch (
                IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}
