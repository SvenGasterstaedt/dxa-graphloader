package de.hhu.bsinfo.dxgraphloader.formats.readers;

import de.hhu.bsinfo.dxgraphloader.graph.data.Vertex;
import de.hhu.bsinfo.dxgraphloader.loader.data.GraphObject;
import de.hhu.bsinfo.dxgraphloader.loader.formats.GraphFormatReader;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class EdgeListReader extends GraphFormatReader {

    public EdgeListReader(GraphObject graphObject, ConcurrentHashMap<String, Long> peerVertexMap, ArrayList<Set<String>> remoteKeys, ChunkLocalService chunkLocalService, BootService bootService) {
        super(graphObject, peerVertexMap, remoteKeys, chunkLocalService, bootService);
    }

    @Override
    public void readVertices(byte[] content) {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(content)));
        try {
            while (bufferedReader.ready()) {
                String line = bufferedReader.readLine();
                if (line.charAt(0) == '#') continue;
                int index = line.indexOf('\t');
                createVertex(line.substring(0, index), Vertex.class);
                int index2 = line.substring(index + 1).indexOf('\t');
                if (index2 != -1) {
                    createVertex(line.substring(index + 1, index + 1 + index2), Vertex.class);
                } else {
                    createVertex(line.substring(index + 1), Vertex.class);
                }
            }
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void readEdges(byte[] content) {
        //createEdge(key1, key2, Edge.class);
    }
}
