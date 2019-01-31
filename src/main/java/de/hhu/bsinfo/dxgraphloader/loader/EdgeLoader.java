package de.hhu.bsinfo.dxgraphloader.loader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.loader.data.Graph;
import de.hhu.bsinfo.dxgraphloader.loader.data.Long2DArray;
import de.hhu.bsinfo.dxgraphloader.loader.data.LongPairArray;
import de.hhu.bsinfo.dxgraphloader.loader.data.PeerVertexMap;
import de.hhu.bsinfo.dxgraphloader.loader.graphobjects.Edge;
import de.hhu.bsinfo.dxgraphloader.loader.graphobjects.Vertex;
import de.hhu.bsinfo.dxgraphloader.util.Barrier;
import de.hhu.bsinfo.dxgraphloader.util.IDUtils;
import de.hhu.bsinfo.dxgraphloader.util.Tuple;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierStatus;

class EdgeLoader {
    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());
    private static final int SYNCARRAYSIZE = 1048576;
    private PeerContext m_context;
    private Queue<Long> m_chunks;
    private Graph m_graph;
    private String m_classpath;
    private AtomicLong m_totalEdgeCount = new AtomicLong();

    EdgeLoader(PeerContext p_context, Queue<Long> p_chunkList, Graph p_graph, String p_classpath) {
        m_context = p_context;
        m_chunks = p_chunkList;
        m_graph = p_graph;
        m_classpath = p_classpath;
    }

    void load() {
        Set<Tuple> edgeTuples = Collections.newSetFromMap(new ConcurrentHashMap<Tuple, Boolean>());
        HashMap<Long, Vertex> vertices = new HashMap<>();
        KeyCreator keyCreator = m_graph.getKeyCreator();

        //synchronization tools
        int loadBarrier = Barrier.getBarrier(GraphLoader.LOAD_LOCK, m_context.getNameserviceService());
        int load2Barrier = Barrier.getBarrier(GraphLoader.LOAD2_LOCK, m_context.getNameserviceService());
        int freeBarrier = Barrier.getBarrier(GraphLoader.FREE_LOCK, m_context.getNameserviceService());

        LOGGER.info("Reading Edge Keys from Chunks!");
        localWorkers(edgeTuples);
        GraphLoader.logMem();

        LOGGER.info("%s Edges on this peer!", edgeTuples.size());

        LOGGER.info("Creating vertices based on 1st Cycle!");
        {
            PeerVertexMap vertexMap = new PeerVertexMap();
            vertexMap.setID(m_graph.getMap(m_context.getBootService().getNodeID()));
            m_context.getChunkLocalService().getLocal().get(vertexMap);

            for (Iterator<Map.Entry<Long, Long>> iterator = vertexMap.getMap().entrySet().iterator();
                    iterator.hasNext(); ) {
                Vertex v = new Vertex();
                vertices.put(iterator.next().getKey(), v);
                m_context.getChunkLocalService().createLocal().create(v);
                iterator.remove();
            }
        }
        GraphLoader.logMem();

        HashMap<Tuple, Edge> sharedEdges = new HashMap<Tuple, Edge>();
        int totalEdges = edgeTuples.size();
        createExclusivEdgesAndSharedEdgesToMap(vertices, edgeTuples, sharedEdges);

        //sort vertices of the edges into a 2dim array where each index stands for peer
        Long2DArray finder = sortVerticesForPeers(sharedEdges);

        //create actual edges
        HashMap<Long, Edge> edgeID = new HashMap<>(sharedEdges.size());
        for (Iterator<Map.Entry<Tuple, Edge>> iterator = sharedEdges.entrySet().iterator(); iterator.hasNext(); ) {
            Edge e = iterator.next().getValue();
            edgeID.put(e.getID(), e);
            iterator.remove();
        }
        GraphLoader.logMem();

        LOGGER.info("Waiting for other peers ...");
        BarrierStatus status = m_context.getSynchronizationService().barrierSignOn(loadBarrier, finder.getID(),
                true);
        GraphLoader.logMem();

        LOGGER.info("Starting Key Resolving!");
        resolveKeys(vertices, status.getCustomData());

        //now all vertices got all edges!
        LOGGER.info("Waiting for other peers ...");
        m_context.getSynchronizationService().barrierSignOn(load2Barrier, finder.getID(), true);

        LOGGER.info("Putting Vertices!");
        vertices.values().forEach(
                v -> {
                    m_context.getChunkService().resize().resize(v);
                    m_context.getChunkService().put().put(v);
                });
        GraphLoader.logMem();

        long localEdges = m_totalEdgeCount.get();
        LOGGER.info("Inserting Vertices into Edges!");
        int peerEdges = addVerticesToEdges(finder, edgeID);

        LOGGER.info("Putting Shared Edges!");
        peerEdges /= 2;
        edgeID.values().forEach(
                e -> {
                    m_context.getChunkService().put().put(e);
                    m_totalEdgeCount.getAndIncrement();
                });

        LOGGER.info("Peer Stats for [%s]", IDUtils.shortToHexString(m_context.getBootService().getNodeID()));
        LOGGER.info("Vertices on this peer:\t%s", vertices.size());
        LOGGER.info("Exclusiv Edges:\t\t%s", localEdges);
        LOGGER.info("Shared Edges:\t\t%s", peerEdges);
        LOGGER.info("Total Edges:\t\t\t%s", totalEdges);

        m_context.getSynchronizationService().barrierSignOn(freeBarrier, finder.getID(), true);

        for (int i = 0; i < finder.getSize(); i++) {
            m_context.getChunkService().remove().remove(Arrays.stream(finder.getArray(i))
                    .filter(l -> l != ChunkID.INVALID_ID && l != 0).toArray());
        }
        m_context.getChunkService().remove().remove(finder);
    }

    private void localWorkers(final Set<Tuple> p_edges) {
        Lock lock = new ReentrantLock(true);
        List<LoadChunkJob> jobList = new ArrayList<>();
        //starting jobs
        for (int i = 0; i < GraphLoader.WORKERCOUNT - 1; i++) {
            LoadChunkJob abstractJob = new LoadChunkJob(m_chunks, m_classpath, lock, m_graph, p_edges, 1);
            jobList.add(abstractJob);
            m_context.getJobService().pushJob(abstractJob);
        }
        //this job is finish and just joins the other jobs and helps loading
        LoadChunkJob localJob = new LoadChunkJob(m_chunks, m_classpath, lock, m_graph, p_edges, 1);
        localJob.setServicesForLocal(m_context);
        jobList.add(localJob);
        localJob.execute();
        PeerManagerJob.waitForJobsToFinish(jobList);
    }

    private void createExclusivEdgesAndSharedEdgesToMap(final HashMap<Long, Vertex> p_vertices,
            final Set<Tuple> p_edges,
            final HashMap<Tuple, Edge> p_sharedEdges) {
        int totalEdges = p_edges.size();
        for (Iterator<Tuple> iterator = p_edges.iterator(); iterator.hasNext(); ) {
            Tuple tuple = iterator.next();
            if (KeyCreator.getPeer(tuple.getX()) == KeyCreator.getPeer(tuple.getY()) &&
                    KeyCreator.getPeer(tuple.getX()) == m_context.getBootService().getNodeID()) {
                Edge e = new Edge();
                m_context.getChunkLocalService().createLocal().create(e);
                Vertex v1 = p_vertices.get(tuple.getX());
                Vertex v2 = p_vertices.get(tuple.getY());
                e.setEndPoint(v1, v2);
                m_context.getChunkService().put().put(e);
                m_totalEdgeCount.getAndIncrement();
                iterator.remove();
            }
        }

        LOGGER.info("Created '%s' local Edges on '%s'", totalEdges - p_edges.size(),
                IDUtils.shortToHexString(m_context.getBootService().getNodeID()));

        //Create Edges, so we can send ids - to vertices
        for (Iterator<Tuple> iterator = p_edges.iterator(); iterator.hasNext(); ) {
            Edge e = new Edge();
            m_context.getChunkLocalService().createLocal().create(e);
            p_sharedEdges.put(iterator.next(), e);
            iterator.remove();
        }
    }

    private Long2DArray sortVerticesForPeers(final HashMap<Tuple, Edge> p_sharedEdges) {
        //1)For every peer
        Long2DArray finder = new Long2DArray(m_graph.getKeyCreator().getPeers().size());
        for (int i = 0; i < m_graph.getKeyCreator().getPeers().size(); i++) {
            int size = 0;
            LOGGER.info("Keys for Peer: '%s'",
                    IDUtils.shortToHexString(m_context.getBootService().getOnlinePeerNodeIDs().get(i)));
            long[] peerIDs = new long[2 * (p_sharedEdges.size() / SYNCARRAYSIZE) + 1];
            Arrays.fill(peerIDs, ChunkID.INVALID_ID);
            int pos = 0;
            int arrayIndex = 0;

            long[] keys = new long[SYNCARRAYSIZE];
            long[] ids = new long[SYNCARRAYSIZE];
            Arrays.fill(keys, ChunkID.INVALID_ID);
            Arrays.fill(ids, ChunkID.INVALID_ID);

            //Ugly but does what it should - stores (VertexKey,EdgeID) "Tuple" in Chunks and send them onto remote
            // peer for resolving - no edges gets lost infact we get in total 2n entries for Edges.
            //2)go over the edges
            for (Iterator<Map.Entry<Tuple, Edge>> iterator = p_sharedEdges.entrySet().iterator();
                    iterator.hasNext(); ) {
                Map.Entry<Tuple, Edge> entry = iterator.next();

                //3) and its keys
                for (int k = 0; k < entry.getKey().getKeys().length; k++) {
                    //check the peer where they belong
                    if (KeyCreator.getPeer(entry.getKey().getKeys()[k]) == m_graph.getKeyCreator().getPeers().get(i)) {
                        //4) put them into an array
                        keys[arrayIndex] = entry.getKey().getKeys()[k];
                        ids[arrayIndex] = entry.getValue().getID();
                        arrayIndex++;
                        size++;
                    }

                    //if array is full create pairArray or we reached the end of the edge list
                    if (arrayIndex >= SYNCARRAYSIZE ||
                            !iterator.hasNext() && k == entry.getKey().getKeys().length - 1) {
                        final LongPairArray pairArray = new LongPairArray(keys, ids);
                        while (pairArray.getID() == ChunkID.INVALID_ID) {
                            m_context.getChunkService().create().create(m_graph.getKeyCreator().getPeers().get(i),
                                    pairArray);
                        }
                        boolean status;
                        do {
                            status = m_context.getChunkService().put().put(pairArray);
                        } while (!status);

                        //put into finder structure
                        peerIDs[pos] = pairArray.getID();
                        pos++;

                        LOGGER.debug("Created %s!", Long.toHexString(pairArray.getID()));
                        Arrays.fill(keys, ChunkID.INVALID_ID);
                        Arrays.fill(ids, ChunkID.INVALID_ID);
                        arrayIndex = 0;
                    }

                }
            }
            LOGGER.info("%s Key-Value-Pairs for Peer: '%s'", size,
                    IDUtils.shortToHexString(m_context.getBootService().getOnlinePeerNodeIDs().get(i)));
            finder.add(i, peerIDs);
        }
        int counter = 0;
        while (finder.getID() == ChunkID.INVALID_ID) {
            m_context.getChunkService().create().create(m_context.getBootService().getNodeID(), finder);
            m_context.getChunkService().put().put(finder);
            if (counter > 10) {
                break;
            }
            counter++;
        }
        return finder;
    }

    private void resolveKeys(final HashMap<Long, Vertex> vertices, final long[] finderIDs) {
        Long2DArray[] finders = new Long2DArray[finderIDs.length];
        for (int i = 0; i < finderIDs.length; i++) {

            finders[i] = new Long2DArray(finderIDs.length, finderIDs[i]);
            m_context.getChunkService().get().get(finders);
            for (long l : Objects.requireNonNull(
                    finders[i].getArray(m_graph.getKeyCreator()
                            .getPeerIndex(m_context.getBootService().getNodeID())))) {
                if (l == ChunkID.INVALID_ID) {
                    continue;
                }
                LOGGER.debug("Resolving '%s'", Long.toHexString(l));
                LongPairArray longPairArray = new LongPairArray(l);
                m_context.getChunkService().get().get(longPairArray);
                long[] keys = longPairArray.getKeys();
                long[] ids = longPairArray.getIDs();
                for (int j = 0; j < keys.length; j++) {
                    if (keys[j] != ChunkID.INVALID_ID || ids[j] != ChunkID.INVALID_ID) {
                        Vertex v = vertices.get(keys[j]);
                        v.addNeighbor(ids[j]);
                        keys[j] = v.getID();
                    }
                }
                m_context.getChunkService().put().put(longPairArray);
            }
        }
    }

    private int addVerticesToEdges(final Long2DArray finder, final HashMap<Long, Edge> edgeID) {
        int count = 0;
        for (int i = 0; i < m_graph.getKeyCreator().getPeers().size(); i++) {
            for (int j = 0; j < finder.getArray(i).length; j++) {
                if (finder.getArray(i)[j] != ChunkID.INVALID_ID) {
                    LongPairArray longPairArray = new LongPairArray(finder.getArray(i)[j]);
                    boolean success;
                    do {
                        success = m_context.getChunkService().get().get(longPairArray);
                    } while (!success);

                    for (int k = 0; k < longPairArray.getKeys().length; k++) {
                        if (longPairArray.getKeys()[k] != ChunkID.INVALID_ID) {
                            Edge e = edgeID.get(longPairArray.getIDs()[k]);
                            e.addConnection(longPairArray.getKeys()[k]);
                            count++;
                        }
                    }
                }
            }
        }
        return count;
    }
}
