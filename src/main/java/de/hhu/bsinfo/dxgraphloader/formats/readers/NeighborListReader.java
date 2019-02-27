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

@SuppressWarnings("Duplicates")
public class NeighborListReader extends GraphFormatReader {


    public NeighborListReader(GraphObject graphObject, ConcurrentHashMap<String, Long> peerVertexMap, ArrayList<Set<String>> remoteKeys, ChunkLocalService chunkLocalService, BootService bootService) {
        super(graphObject, peerVertexMap, remoteKeys, chunkLocalService, bootService);
    }

    @Override
    public void readVertices(byte[] content) {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(content)));
        try {
            char[] lineChar = new char[4];
            bufferedReader.read(lineChar);
            long lineNumber = Long.getLong(new String(lineChar));
            while (bufferedReader.ready()) {
                String line = bufferedReader.readLine();
                if (line.charAt(0) == '#') continue;
                String[] key = line.split("\t");
                String key1 = Long.toString(lineNumber);
                createVertex(key1, Vertex.class);
                for (int i = 0; i < key.length; i++) {
                    createVertex(key[i], Vertex.class);
                }
                lineNumber++;
            }
            bufferedReader.close();
        } catch (
                IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void readEdges(byte[] content) {
    }
}
