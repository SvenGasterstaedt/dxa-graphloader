package de.hhu.bsinfo.dxgraphloader.loader.formats;

import de.hhu.bsinfo.dxgraphloader.graph.data.Edge;
import de.hhu.bsinfo.dxgraphloader.graph.data.Vertex;
import de.hhu.bsinfo.dxgraphloader.loader.data.DistributedObjectTable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public abstract class GraphFormatReader {

    private DistributedObjectTable distributedObjectTable;
    private List<Set<String>> vertices = new ArrayList<>();
    private Set<Edge> edges = new HashSet<>();


    public GraphFormatReader(DistributedObjectTable distributedObjectTable) {
        this.distributedObjectTable = distributedObjectTable;
        for (int i = 0; i < distributedObjectTable.getPeerSize(); i++) {
            vertices.add(new ConcurrentSkipListSet<>());
        }
    }

    public abstract boolean execute(final byte[] content);



    protected void createVertex(String id, Class<? extends Vertex> vertex, String... v_args) {
        short value = distributedObjectTable.getNode(id.hashCode() % distributedObjectTable.getPeerSize());
        vertices.get(distributedObjectTable.getPeerPos(value)).add(id);
    }

    public List<Set<String>> getVertices() {
        return vertices;
    }

    public Set<Edge> getEdges() {
        return edges;
    }
}
