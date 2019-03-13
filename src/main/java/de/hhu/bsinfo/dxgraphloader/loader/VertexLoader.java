package de.hhu.bsinfo.dxgraphloader.loader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.loader.data.Graph;
import de.hhu.bsinfo.dxgraphloader.loader.data.KeyArray;
import de.hhu.bsinfo.dxgraphloader.loader.data.Long2DArray;
import de.hhu.bsinfo.dxgraphloader.loader.data.PeerVertexMap;
import de.hhu.bsinfo.dxgraphloader.util.Barrier;
import de.hhu.bsinfo.dxgraphloader.util.IDUtils;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierStatus;

class VertexLoader {
    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());
    private PeerContext m_context;
    private Queue<Long> m_chunks;
    private Graph m_graph;
    private String m_classpath;

    VertexLoader(PeerContext p_context, Queue<Long> p_chunkList, Graph p_graph, String p_classpath) {
        m_context = p_context;
        m_chunks = p_chunkList;
        m_graph = p_graph;
        m_classpath = p_classpath;
    }

    void load() {
        //list to keep track of our jobs
        //List of Sets for keys, which will be distributed. For Threadsafty this is based on a ConcurrentHashMap
        Set<Long> vertices = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
        ArrayList<Set<Long>> remoteKeySet = new ArrayList<>();
        for (int j = 0; j < m_context.getBootService().getOnlinePeerNodeIDs().size(); j++) {
            remoteKeySet.add(Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>()));
        }

        //synchronization tools
        int loadBarrier = Barrier.getBarrier(GraphLoader.LOAD_LOCK, m_context.getNameserviceService());

        LOGGER.info("Reading Vertex Keys from Chunks!");
        localWorkers(remoteKeySet);
        GraphLoader.logMem();

        LOGGER.info("Preparing Remote Keys!");
        Long2DArray remoteKeyArray = extractRemoteKeys(vertices, remoteKeySet);
        GraphLoader.logMem();

        LOGGER.info("Pushing Remote Keys to KVS!");
        while (remoteKeyArray.getID() == ChunkID.INVALID_ID) {
            m_context.getChunkLocalService().createLocal().create(remoteKeyArray);
            m_context.getChunkService().put().put(remoteKeyArray);

        }
        GraphLoader.logMem();

        LOGGER.info("Waiting for other peers ...");
        BarrierStatus status = m_context.getSynchronizationService()
                .barrierSignOn(loadBarrier, remoteKeyArray.getID(), true);

        LOGGER.info("Synchronization Vertices!");
        long[] allRemoteKeys = status.getCustomData();
        createRemoteKeys(vertices, allRemoteKeys);
        GraphLoader.logMem();

        LOGGER.info("Storing Vertex Keys!");
        HashMap<Long, Long> map = new HashMap<>(vertices.size());
        for (Iterator<Long> iterator = vertices.iterator(); iterator.hasNext(); ) {
            map.put(iterator.next(), ChunkID.INVALID_ID);
            iterator.remove();
        }
        PeerVertexMap vertexMap = new PeerVertexMap(map);
        vertexMap.setID(m_graph.getMap(m_context.getBootService().getNodeID()));
        m_context.getChunkService().resize().resize(vertexMap);
        m_context.getChunkService().put().put(vertexMap);

        LOGGER.info("'%s' holds '%s' Vertices Keys",
                IDUtils.shortToHexString(m_context.getBootService().getNodeID()), vertices.size());
        GraphLoader.logMem();
    }

    private void localWorkers(final ArrayList<Set<Long>> p_remoteKeySet) {
        Lock lock = new ReentrantLock(true);
        List<LoadChunkJob> jobList = new ArrayList<>();
        //starting jobs
        for (int i = 0; i < GraphLoader.WORKERCOUNT - 1; i++) {
            LoadChunkJob abstractJob = new LoadChunkJob(m_chunks, m_classpath, lock, m_graph, p_remoteKeySet, 0);
            jobList.add(abstractJob);
            m_context.getJobService().pushJob(abstractJob);
        }
        //this job is finish and just joins the other jobs and helps loading
        LoadChunkJob localJob = new LoadChunkJob(m_chunks, m_classpath, lock, m_graph, p_remoteKeySet, 0);
        localJob.setServicesForLocal(m_context);
        jobList.add(localJob);
        localJob.execute();
        PeerManagerJob.waitForJobsToFinish(jobList);
    }

    private Long2DArray extractRemoteKeys(final Set<Long> p_vertices, final ArrayList<Set<Long>> p_remoteKeySet) {
        Long2DArray finder = new Long2DArray(m_context.getBootService().getOnlinePeerNodeIDs().size());
        for (int i = 0; i < m_context.getBootService().getOnlinePeerNodeIDs().size(); i++) {
            if (m_context.getBootService().getOnlinePeerNodeIDs().get(i) == m_context.getBootService().getNodeID()) {
                p_vertices.addAll(p_remoteKeySet.get(i));
                finder.add(i, new long[]{ChunkID.INVALID_ID});
                continue;
            }

            Set<Long> set = p_remoteKeySet.get(i);
            long[] ids = new long[set.size() / 5324];
            Arrays.fill(ids, ChunkID.INVALID_ID);
            LOGGER.info("Keys for Peer: '%s'",
                    IDUtils.shortToHexString(m_context.getBootService().getOnlinePeerNodeIDs().get(i)));
            int pos = 0;
            HashSet<Long> smallSet = new HashSet<>();
            Iterator<Long> iterator = set.iterator();

            while (iterator.hasNext()) {

                smallSet.add(iterator.next());

                if (smallSet.size() > 5324 * 32 || !iterator.hasNext()) {

                    final KeyArray keyArray = new KeyArray(smallSet);

                    while (keyArray.getID() == ChunkID.INVALID_ID) {
                        m_context.getChunkService().create().create(
                                m_context.getBootService().getOnlinePeerNodeIDs().get(i), keyArray);
                    }

                    boolean status;

                    do {
                        status = m_context.getChunkService().put().put(keyArray);
                    } while (!status);

                    LOGGER.debug("Created %s!", Long.toHexString(keyArray.getID()));
                    ids[pos] = keyArray.getID();
                    pos++;
                    smallSet = new HashSet<>();
                }
            }

            finder.add(i, ids);
        }
        return finder;
    }

    private void createRemoteKeys(final Set<Long> p_vertices, final long[] p_remoteKeyArraysId) {
        Long2DArray[] finders = new Long2DArray[p_remoteKeyArraysId.length];

        for (int i = 0; i < p_remoteKeyArraysId.length; i++) {
            if(p_remoteKeyArraysId[i]==ChunkID.INVALID_ID){
                continue;
            }
            finders[i] = new Long2DArray(p_remoteKeyArraysId.length, p_remoteKeyArraysId[i]);
            m_context.getChunkService().get().get(finders);
            for (long l : finders[i].getArray(m_graph.getKeyCreator().getPeerIndex(m_context.getBootService().getNodeID()))) {
                if (l == ChunkID.INVALID_ID) {
                    continue;
                }
                LOGGER.debug("Stored '%s'", Long.toHexString(l));
                KeyArray keyArray = new KeyArray(l);
                m_context.getChunkService().get().get(keyArray);
                Arrays.stream(keyArray.getKeys()).forEach(p_vertices::add);
                m_context.getChunkService().remove().remove(keyArray);
            }
        }
        m_context.getChunkService().remove().remove(p_remoteKeyArraysId);
    }
}
