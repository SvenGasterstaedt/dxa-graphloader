package de.hhu.bsinfo.dxgraphloader.graph.data;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.util.Arrays;

public class Vertex extends AbstractChunk {

    private long[] edges;


    public Vertex(long p_id) {
        edges = new long[1];
        setID(p_id);
    }

    public Vertex() {
        edges = new long[1];
    }

    public Vertex(long p_id, int neighbor_count) {
        super(p_id);
        edges = new long[neighbor_count];
    }

    public void addNeighbor(long edgeID) {
        edges = Arrays.copyOf(edges, edges.length + 1);
        edges[edges.length - 1] = edgeID;
    }

    public void addNeighbors(long[] edgeIDs) {
        edges = Arrays.copyOf(edges, edges.length + 1);
        System.arraycopy(edgeIDs, 0, edges, edges.length, edgeIDs.length);
    }

    public void mergeVertex(Vertex vertex, ChunkService chunkService) {
        for (long l : vertex.edges) {
            Edge edge = new Edge(l);
            chunkService.get().get(edge);
            if (edge.from == vertex.getID()) {
                edge.setFrom(this.getID());
            } else {
                edge.setTo(this.getID());
            }
            chunkService.put().put(edge);
        }
        this.addNeighbors(vertex.edges);
        chunkService.remove().remove(vertex);
        chunkService.resize().resize(this);
        chunkService.put().put(this);
    }

    @Override
    public void exportObject(Exporter exporter) {
        exporter.writeInt(edges.length);
        for (long l : edges) {
            exporter.writeLong(l);
        }
    }

    @Override
    public void importObject(Importer importer) {
        int size = 0;
        size = importer.readInt(size);
        edges = new long[size];
        for (int i = 0; i < size; i++) {
            edges[i] = importer.readLong(edges[i]);
        }
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + Long.BYTES * edges.length;
    }
}
