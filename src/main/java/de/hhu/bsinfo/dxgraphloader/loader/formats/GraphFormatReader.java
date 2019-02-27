package de.hhu.bsinfo.dxgraphloader.loader.formats;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.graph.data.Edge;
import de.hhu.bsinfo.dxgraphloader.graph.data.Vertex;
import de.hhu.bsinfo.dxgraphloader.loader.data.GraphObject;
import de.hhu.bsinfo.dxgraphloader.util.Tuple;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class GraphFormatReader {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());

    private GraphObject graphObject;

    private Set<Tuple<String, String>> edges = new HashSet<>();
    private ConcurrentHashMap<String, Long> peerVertexMap;
    ArrayList<Set<String>> remoteKeys;
    private ChunkLocalService chunkLocalService;
    private BootService bootService;
    private Class<? extends Vertex> vertex;

    public GraphFormatReader(GraphObject graphObject, ConcurrentHashMap<String, Long> peerVertexMap, ArrayList<Set<String>> remoteKeys, ChunkLocalService chunkLocalService, BootService bootService) {
        this.graphObject = graphObject;
        this.peerVertexMap = peerVertexMap;
        this.remoteKeys = remoteKeys;
        this.chunkLocalService = chunkLocalService;
        this.bootService = bootService;
    }

    public abstract void readVertices(final byte[] content);

    public abstract void readEdges(final byte[] content);

    protected void createVertex(String id, Object... v_args) {
        Vertex vertex = new Vertex();
        short targetPeer = graphObject.getNode(id.hashCode() % graphObject.getPeerSize());
        if (targetPeer == bootService.getNodeID()) {
            Long value = peerVertexMap.putIfAbsent(id, ChunkID.INVALID_ID);
            if (value == null) {
                chunkLocalService.createLocal().create(vertex);
                peerVertexMap.replace(id, vertex.getID());
            }
        } else {
           remoteKeys.get(graphObject.getPeerPos(targetPeer)).add(id);
        }
    }


    protected void createEdge(String from, String to, Class<? extends Edge> edge, Object... v_args) {
        edges.add(new Tuple<>(from, to));
    }

    public Set<Tuple<String, String>> getEdges() {
        return edges;
    }


}
