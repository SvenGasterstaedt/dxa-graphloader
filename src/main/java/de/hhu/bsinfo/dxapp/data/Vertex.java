package de.hhu.bsinfo.dxapp.data;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.Arrays;

public class Vertex extends AbstractChunk {

    protected long[] edges;

    public Vertex(long p_id){
        super(p_id);
        edges = new long[0];
    }

    public Vertex(){
        edges = new long[0];
    }

    public Vertex(long p_id, int neighbor_count){
        super(p_id);
        edges = new long[neighbor_count];
    }

    public void addNeighbor(long edgeID){
        edges = Arrays.copyOf(edges,edges.length+1);
        edges[edges.length-1] = edgeID;
    }

    public void addNeighbors(long[] edgeIDs){
        edges = Arrays.copyOf(edges,edges.length+1);
        System.arraycopy(edgeIDs,0,edges,edges.length,edgeIDs.length);
    }


    @Override
    public void exportObject(Exporter exporter) {
        exporter.writeLongArray(edges);
    }

    @Override
    public void importObject(Importer importer) {
        importer.readLongArray(edges);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofLongArray(edges);
    }
}
