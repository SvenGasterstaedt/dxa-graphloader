/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxgraphloader.loader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.loader.data.Graph;
import de.hhu.bsinfo.dxgraphloader.loader.data.KeyArray;
import de.hhu.bsinfo.dxgraphloader.loader.data.Long2DArray;
import de.hhu.bsinfo.dxgraphloader.loader.data.LongArray;
import de.hhu.bsinfo.dxgraphloader.loader.data.LongPairArray;
import de.hhu.bsinfo.dxgraphloader.loader.data.PeerVertexMap;
import de.hhu.bsinfo.dxgraphloader.loader.graphobjects.Edge;
import de.hhu.bsinfo.dxgraphloader.loader.graphobjects.Vertex;
import de.hhu.bsinfo.dxgraphloader.util.Barrier;
import de.hhu.bsinfo.dxgraphloader.util.IDUtils;
import de.hhu.bsinfo.dxgraphloader.util.Tuple;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierStatus;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Main Job Class for the GraphLoader. Manges Jobs/Synchronization and much more.
 */
@SuppressWarnings("Duplicates")
public final class PeerManagerJob extends AbstractJob {
    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());

    private int m_workerCount;
    private int m_cycle;

    private long m_graph;
    private LongArray m_longArray = new LongArray();
    private String m_classpath;

    /**
     * Needed for im- and exporting, to set the ID.
     */
    public PeerManagerJob() {
        super();
    }

    /**
     * Constructor
     *
     * @param p_graphID
     *         graph object id from starting application
     * @param p_longArray
     *         array of file chunks
     * @param p_graphLoader
     *         graphloader's reader
     * @param p_workerCount
     *         amount of jobs to be created
     * @param p_cycle
     *         the cycle we are in (Vertex/Edge cycle)
     */
    public PeerManagerJob(final long p_graphID, final LongArray p_longArray,
            final String p_graphLoader, final int p_workerCount, int p_cycle) {
        m_longArray = p_longArray;
        m_classpath = p_graphLoader;
        m_graph = p_graphID;
        m_workerCount = p_workerCount;
        m_cycle = p_cycle;
    }

    /**
     * This function is run by the JobService.class on the remote peer
     */
    @Override
    public void execute() {
        LOGGER.info("Started '%s' from '%s'!",
                PeerManagerJob.class.getSimpleName(),
                GraphLoaderApp.class.getSimpleName());

        //GraphContext to access Services in Functions etc.
        final PeerContext context = new PeerContext(getService(BootService.class),
                getService(ChunkService.class),
                getService(ChunkLocalService.class),
                getService(JobService.class),
                getService(NameserviceService.class),
                getService(SynchronizationService.class));

        //Main Graph Object
        Graph graph = new Graph(m_graph);
        context.getChunkService().get().get(graph);

        //Queue with Chunks of the File
        Queue<Long> chunkList = Arrays.stream(
                m_longArray.getIds())
                .boxed()
                .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
        context.getChunkService().remove().remove(m_longArray);

        //Barrier to indicate end of job
        int cycleBarrier = Barrier.getBarrier(GraphLoader.CYCLE_LOCK, context.getNameserviceService());

        //Function to execute depend on which cycle we are
        switch (m_cycle) {
            case 0:
                //loading vertices and store them in peerArray - then sync with other peers
                loadVertices(context, chunkList, graph);
                break;
            case 1:
                //loading edges and insert them into Vertices local and remote
                loadEdges(context, chunkList, graph);
                break;
            default:
                //nothing :-)
                break;
        }

        //release barrier and end the cycle - wait for all other peers to finish
        context.getSynchronizationService().barrierSignOn(cycleBarrier, ChunkID.INVALID_ID, true);
    }

    /**
     * This Function loads Vertices from chunks and synchronizes them between peers.
     *
     * @param p_context
     *         local peer context
     * @param p_chunkList
     *         list of file chunks (needs to be threadsafe)
     * @param p_graph
     *         graph object
     */
    private void loadVertices(PeerContext p_context, Queue<Long> p_chunkList, Graph p_graph) {
        //list to keep track of our jobs
        List<LoadChunkJob> jobList = new ArrayList<>();

        //List of Sets for keys, which will be distributed. For Threadsafty this is based on a ConcurrentHashMap
        Set<Long> vertices = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
        ArrayList<Set<Long>> remoteKeySet = new ArrayList<>();
        for (int j = 0; j < p_context.getBootService().getOnlinePeerNodeIDs().size(); j++) {
            remoteKeySet.add(Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>()));
        }

        //synchronization tools
        int loadBarrier = Barrier.getBarrier(GraphLoader.LOAD_LOCK, p_context.getNameserviceService());
        Lock lock = new ReentrantLock(true);

        LOGGER.info("Extracting and Creating Local Vertices!");

        //starting jobs
        for (int i = 0; i < m_workerCount - 1; i++) {
            LoadChunkJob abstractJob = new LoadChunkJob(p_chunkList, m_classpath, lock, p_graph, remoteKeySet, m_cycle);
            jobList.add(abstractJob);
            p_context.getJobService().pushJob(abstractJob);
        }
        //this job is finish and just joins the other jobs and helps loading
        LoadChunkJob localJob = new LoadChunkJob(p_chunkList, m_classpath, lock, p_graph, remoteKeySet, m_cycle);
        localJob.setServicesForLocal(p_context);
        jobList.add(localJob);
        localJob.execute();

        waitForJobsToFinish(jobList);

        LOGGER.info("Storing Remote Keys!");
        Long2DArray finder = new Long2DArray(p_context.getBootService().getOnlinePeerNodeIDs().size());

        for (int i = 0; i < p_context.getBootService().getOnlinePeerNodeIDs().size(); i++) {
            if (p_context.getBootService().getOnlinePeerNodeIDs().get(i) == p_context.getBootService().getNodeID()) {
                vertices.addAll(remoteKeySet.get(i));
                continue;
            }

            Set<Long> set = remoteKeySet.get(i);
            long[] ids = new long[set.size() / 5324];
            Arrays.fill(ids, ChunkID.INVALID_ID);
            LOGGER.info("Keys for Peer: '%s'",
                    IDUtils.shortToHexString(p_context.getBootService().getOnlinePeerNodeIDs().get(i)));
            int pos = 0;
            HashSet<Long> smallSet = new HashSet<>();
            Iterator<Long> iterator = set.iterator();

            while (iterator.hasNext()) {

                smallSet.add(iterator.next());

                if (smallSet.size() > 5324 * 32 || !iterator.hasNext()) {

                    final KeyArray keyArray = new KeyArray(smallSet);

                    while (keyArray.getID() == ChunkID.INVALID_ID) {
                        p_context.getChunkService().create().create(
                                p_context.getBootService().getOnlinePeerNodeIDs().get(i), keyArray);
                    }

                    boolean status;

                    do {
                        status = p_context.getChunkService().put().put(keyArray);
                    } while (!status);

                    LOGGER.debug("Created %s!", Long.toHexString(keyArray.getID()));
                    ids[pos] = keyArray.getID();
                    pos++;
                    smallSet = new HashSet<>();
                }
            }

            finder.add(i, ids);
        }

        while (finder.getID() == ChunkID.INVALID_ID) {
            p_context.getChunkService().create().create(p_context.getBootService().getNodeID(), finder);
            p_context.getChunkService().put().put(finder);

        }

        LOGGER.info("Waiting for other peers ...");
        BarrierStatus status = p_context.getSynchronizationService()
                .barrierSignOn(loadBarrier, finder.getID(), true);

        LOGGER.info("Starting Synchronization!");
        long[] finderIDs = status.getCustomData();
        Long2DArray[] finders = new Long2DArray[finderIDs.length];

        for (int i = 0; i < finderIDs.length; i++) {

            finders[i] = new Long2DArray(finderIDs[i]);
            p_context.getChunkService().get().get(finders);

            for (long l : Objects.requireNonNull(
                    finders[i]
                            .getArray(p_graph.getKeyCreator().getPeerIndex(p_context.getBootService().getNodeID())))) {
                if (l == ChunkID.INVALID_ID) {

                    continue;
                }
                LOGGER.debug("Stored '%s'", Long.toHexString(l));
                KeyArray keyArray = new KeyArray(l);
                p_context.getChunkService().get().get(keyArray);
                Arrays.stream(keyArray.getKeys()).forEach(vertices::add);
                p_context.getChunkService().remove().remove(keyArray);
            }
        }
        p_context.getChunkService().remove().remove(finder);

        //store vertices in map - for reference
        HashMap<Long, Long> map = new HashMap<>(vertices.size());
        vertices.forEach(key -> map.put(key, ChunkID.INVALID_ID));
        PeerVertexMap vertexMap = new PeerVertexMap(map);
        vertexMap.setID(p_graph.getMap(p_context.getBootService().getNodeID()));
        p_context.getChunkService().resize().resize(vertexMap);
        p_context.getChunkService().put().put(vertexMap);

        LOGGER.info("'%s' holds '%s' Vertices keys",
                IDUtils.shortToHexString(p_context.getBootService().getNodeID()),
                vertices.size());
    }

    /**
     * This Function loads Edges from chunks and synchronizes them between peers.
     * Must be run after Vertices are available.
     *
     * @param p_context
     *         local peer context
     * @param p_chunkList
     *         list of file chunks (needs to be threadsafe)
     * @param p_graph
     *         graph object
     */
    private void loadEdges(PeerContext p_context, Queue<Long> p_chunkList, Graph p_graph) {
        Set<Tuple> m_edges = Collections.newSetFromMap(new ConcurrentHashMap<Tuple, Boolean>());
        //synchronization tools
        int loadBarrier = Barrier.getBarrier(GraphLoader.LOAD_LOCK, p_context.getNameserviceService());
        int load2Barrier = Barrier.getBarrier(GraphLoader.LOAD2_LOCK, p_context.getNameserviceService());

        AtomicLong totalEdgeCount = new AtomicLong();

        //list to keep track of our jobs
        List<LoadChunkJob> jobList = new ArrayList<>();
        Lock lock = new ReentrantLock(true);

        LOGGER.info("Loading Edges!");
        //starting jobs
        for (int i = 0; i < m_workerCount - 1; i++) {
            LoadChunkJob abstractJob = new LoadChunkJob(p_chunkList, m_classpath, lock, p_graph, m_edges, m_cycle);
            jobList.add(abstractJob);
            p_context.getJobService().pushJob(abstractJob);
        }
        //this job is finish and just joins the other jobs and helps loading
        LoadChunkJob localJob = new LoadChunkJob(p_chunkList, m_classpath, lock, p_graph, m_edges, m_cycle);
        localJob.setServicesForLocal(p_context);
        jobList.add(localJob);
        localJob.execute();

        waitForJobsToFinish(jobList);

        LOGGER.info("%s Edges on this peer!", m_edges.size());
        //local peer vertex map
        PeerVertexMap vertexMap = new PeerVertexMap();
        vertexMap.setID(p_graph.getMap(p_context.getBootService().getNodeID()));
        p_context.getChunkLocalService().getLocal().get(vertexMap);
        Map<Long, Long> map = vertexMap.getMap();

        //key service
        KeyCreator keyCreator = p_graph.getKeyCreator();

        //remote map
        HashMap<Long, Vertex> vertices = new HashMap<>(map.size());
        for (Iterator<Map.Entry<Long, Long>> i = map.entrySet().iterator(); i.hasNext(); ) {
            Vertex v = new Vertex();
            vertices.put(i.next().getKey(), v);
            p_context.getChunkLocalService().createLocal().create(v);
            i.remove();
        }

        int totalEdges = m_edges.size();

        for (Iterator<Tuple> i = m_edges.iterator(); i.hasNext(); ) {
            Tuple tuple = i.next();
            if (KeyCreator.getPeer(tuple.getX()) == KeyCreator.getPeer(tuple.getY()) &&
                    KeyCreator.getPeer(tuple.getX()) == p_context.getBootService().getNodeID()) {
                Edge e = new Edge();
                p_context.getChunkLocalService().createLocal().create(e);
                Vertex v1 = vertices.get(tuple.getX());
                Vertex v2 = vertices.get(tuple.getY());
                e.setEndPoint(v1, v2);
                p_context.getChunkService().put().put(e);
                totalEdgeCount.getAndIncrement();
                i.remove();
            }
        }

        LOGGER.info("Created '%s' local Edges on '%s'", totalEdges - m_edges.size(),
                IDUtils.shortToHexString(p_context.getBootService().getNodeID()));

        //Create Edges, so we can send ids - to vertices
        HashMap<Tuple, Edge> edges = new HashMap<>(m_edges.size());
        for (Iterator<Tuple> i = m_edges.iterator(); i.hasNext(); ) {
            Edge e = new Edge();
            p_context.getChunkLocalService().createLocal().create(e);
            edges.put(i.next(), e);
            i.remove();
        }

        //1)For every peer
        Long2DArray finder = new Long2DArray(keyCreator.getPeers().size());
        for (int i = 0; i < keyCreator.getPeers().size(); i++) {
            int size = 0;
            LOGGER.info("Keys for Peer: '%s'",
                    IDUtils.shortToHexString(p_context.getBootService().getOnlinePeerNodeIDs().get(i)));
            long[] peerIDs = new long[2 * (edges.size() / 5324) + 1];
            Arrays.fill(peerIDs, ChunkID.INVALID_ID);
            int pos = 0;
            int arrayIndex = 0;

            long[] keys = new long[5324];
            long[] ids = new long[5324];
            Arrays.fill(keys, ChunkID.INVALID_ID);
            Arrays.fill(ids, ChunkID.INVALID_ID);

            //Ugly but does what it should - stores (VertexKey,EdgeID) "Tuple" in Chunks and send them onto remote
            // peer for resolving - no edges gets lost infact we get in total 2n entries for Edges.
            //2)go over the edges
            for (Iterator<Map.Entry<Tuple, Edge>> iterator = edges.entrySet().iterator();
                    iterator.hasNext(); ) {
                Map.Entry<Tuple, Edge> entry = iterator.next();

                //3) and its keys
                for (int k = 0; k < entry.getKey().getKeys().length; k++) {
                    //check the peer where they belong
                    if (KeyCreator.getPeer(entry.getKey().getKeys()[k]) == keyCreator.getPeers().get(i)) {
                        //4) put them into an array
                        keys[arrayIndex] = entry.getKey().getKeys()[k];
                        ids[arrayIndex] = entry.getValue().getID();
                        arrayIndex++;
                        size++;
                    }

                    //if array is full create pairArray or we reached the end of the edge list
                    if (arrayIndex >= 5324 || !iterator.hasNext() && k == entry.getKey().getKeys().length - 1) {
                        final LongPairArray pairArray = new LongPairArray(keys, ids);
                        while (pairArray.getID() == ChunkID.INVALID_ID) {
                            p_context.getChunkService().create().create(keyCreator.getPeers().get(i), pairArray);
                        }
                        boolean status;
                        do {
                            status = p_context.getChunkService().put().put(pairArray);
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
                    IDUtils.shortToHexString(p_context.getBootService().getOnlinePeerNodeIDs().get(i)));
            finder.add(i, peerIDs);
        }
        while (finder.getID() == ChunkID.INVALID_ID) {
            p_context.getChunkService().create().create(p_context.getBootService().getNodeID(), finder);
            p_context.getChunkService().put().put(finder);
        }

        HashMap<Long, Edge> edgeID = new HashMap<>(edges.size());
        for (Iterator<Map.Entry<Tuple, Edge>> iterator = edges.entrySet().iterator(); iterator.hasNext(); ) {
            Edge e = iterator.next().getValue();
            edgeID.put(e.getID(), e);
            iterator.remove();
        }

        LOGGER.info("Waiting for other peers ...");
        BarrierStatus status = p_context.getSynchronizationService().barrierSignOn(loadBarrier, finder.getID(),
                true);

        {
            LOGGER.info("Starting Key Resolving!");
            long[] finderIDs = status.getCustomData();
            Long2DArray[] finders = new Long2DArray[finderIDs.length];
            for (int i = 0; i < finderIDs.length; i++) {

                finders[i] = new Long2DArray(finderIDs[i]);
                p_context.getChunkService().get().get(finders);
                for (long l : Objects.requireNonNull(
                        finders[i].getArray(p_graph.getKeyCreator()
                                .getPeerIndex(p_context.getBootService().getNodeID())))) {
                    if (l == ChunkID.INVALID_ID) {
                        continue;
                    }
                    LOGGER.debug("Resolving '%s'", Long.toHexString(l));
                    LongPairArray longPairArray = new LongPairArray(l);
                    p_context.getChunkService().get().get(longPairArray);
                    long[] keys = longPairArray.getKeys();
                    long[] ids = longPairArray.getIDs();
                    for (int j = 0; j < keys.length; j++) {
                        if (keys[j] != ChunkID.INVALID_ID || ids[j] != ChunkID.INVALID_ID) {
                            Vertex v = vertices.get(keys[j]);
                            v.addNeighbor(ids[j]);
                            keys[j] = v.getID();
                        }
                    }
                    p_context.getChunkService().put().put(longPairArray);
                }
            }
        }

        //now all vertices got all edges!
        LOGGER.info("Waiting for other peers ...");
        status = p_context.getSynchronizationService().barrierSignOn(load2Barrier, finder.getID(),
                true);

        LOGGER.info("Putting Vertices!");
        vertices.values().forEach(
                v -> {
                    p_context.getChunkService().resize().resize(v);
                    p_context.getChunkService().put().put(v);
                });

        long localEdges = totalEdgeCount.get();
        int peerEdges = 0;
        LOGGER.debug("Inserting Vertices into Edges!");
        for (int i = 0; i < keyCreator.getPeers().size(); i++) {
            for (int j = 0; j < finder.getArray(i).length; j++) {
                if (finder.getArray(i)[j] != ChunkID.INVALID_ID) {
                    LongPairArray longPairArray = new LongPairArray(finder.getArray(i)[j]);
                    boolean success;
                    do {
                        success = p_context.getChunkService().get().get(longPairArray);
                    } while (!success);

                    for (int k = 0; k < longPairArray.getKeys().length; k++) {
                        if (longPairArray.getKeys()[k] != ChunkID.INVALID_ID) {
                            Edge e = edgeID.get(longPairArray.getIDs()[k]);
                            e.addConnection(longPairArray.getKeys()[k]);
                            peerEdges++;
                        }
                    }
                    //p_context.getChunkService().remove().remove(longPairArray);
                }
            }
        }
        LOGGER.info("Putting Shared Edges!");
        peerEdges /= 2;
        edgeID.values().forEach(
                e -> {
                    p_context.getChunkService().put().put(e);
                    totalEdgeCount.getAndIncrement();
                });

        LOGGER.info("Peer Stats for [%s]", IDUtils.shortToHexString(p_context.getBootService().getNodeID()));
        LOGGER.info("Vertices on this peer:\t%s", vertices.size());
        LOGGER.info("Exclusiv Edges:\t\t%s", localEdges);
        LOGGER.info("Shared Edges:\t\t%s", peerEdges);
        LOGGER.info("Total Edges:\t\t\t%s", totalEdges);
    }

    private static boolean areAllJobsFinished(List<LoadChunkJob> p_job) {
        for (LoadChunkJob job : p_job) {
            if (!job.isFinished()) {

                return false;
            }
        }

        return true;
    }

    public void waitForJobsToFinish(List<LoadChunkJob> p_job) {
        //Waiting for all jobs in the list to finish loading.
        while (!areAllJobsFinished(p_job)) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        p_job.clear();
    }

    @Override
    public void importObject(final Importer p_importer) {

        super.importObject(p_importer);
        p_importer.importObject(m_longArray);
        m_classpath = p_importer.readString(m_classpath);
        m_workerCount = p_importer.readInt(m_workerCount);
        m_graph = p_importer.readLong(m_graph);
        m_cycle = p_importer.readInt(m_cycle);

    }

    @Override
    public void exportObject(final Exporter p_exporter) {

        super.exportObject(p_exporter);
        p_exporter.exportObject(m_longArray);
        p_exporter.writeString(m_classpath);
        p_exporter.writeInt(m_workerCount);
        p_exporter.writeLong(m_graph);
        p_exporter.writeInt(m_cycle);

    }

    @Override
    public int sizeofObject() {

        return super.sizeofObject() + ObjectSizeUtil.sizeofString(m_classpath) + Integer.BYTES +
                Long.BYTES + Integer.BYTES + m_longArray.sizeofObject();

    }
}