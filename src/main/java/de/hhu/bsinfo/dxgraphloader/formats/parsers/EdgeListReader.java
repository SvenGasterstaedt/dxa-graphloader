package de.hhu.bsinfo.dxgraphloader.formats.parsers;

import de.hhu.bsinfo.dxgraphloader.graph.data.Edge;
import de.hhu.bsinfo.dxgraphloader.graph.data.Vertex;
import de.hhu.bsinfo.dxram.chunk.ChunkService;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

//this example loader doesnt support weighted edges.
//this can easily fixed by just inherteing this class and writing a custom loader with custom data structes;

public class EdgeListReader extends SimpleFormatReader {
    @Override
    public boolean execute(byte[] content, final ChunkService chunkService, final short current_peer) {
        Map<String, Vertex> vertexMap = new HashMap<String, Vertex>();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(content)));
        try {

            while (bufferedReader.ready()) {
                String line = bufferedReader.readLine();
                if (line.charAt(0) == '#') continue;
                String[] key = line.split("\t");
                if (key.length > 1) {
                    Vertex vertex1;
                    if (vertexMap.containsKey(key[0])) {
                        vertex1 = vertexMap.get(key[0]);
                    } else {
                        vertex1 = new Vertex();
                        vertexMap.put(key[0], vertex1);
                        chunkService.create().create(current_peer, vertex1);
                        chunkService.put().put(vertex1);
                    }
                    Vertex vertex2;

                    if (vertexMap.containsKey(key[1])) {
                        vertex2 = vertexMap.get(key[0]);
                    } else {
                        vertex2 = new Vertex();
                        vertexMap.put(key[1], vertex2);
                        chunkService.create().create(current_peer, vertex2);
                        chunkService.put().put(vertex2);
                    }

                    Edge edge = new Edge();
                    chunkService.create().create(current_peer, edge);
                    edge.setFrom(vertex1.getID());
                    edge.setTo(vertex2.getID());
                    vertex1.addNeighbor(edge.getID());
                    vertex2.addNeighbor(edge.getID());
                    chunkService.put().put(edge);
                }
                //System.out.println(key[0] + " to " + key[1] + " created!");
            }
            /*for(Map.Entry<String,Vertex> entry: vertex_id_map.entrySet()){
                Vertex v = entry.getValue();
                m_cs.create(v);
                m_cs.put(v);
            }*/
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}
