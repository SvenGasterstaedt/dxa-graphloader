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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.graphobjects.Vertex;
import de.hhu.bsinfo.dxgraphloader.loader.data.GraphObject;
import de.hhu.bsinfo.dxgraphloader.loader.data.KeyArray;
import de.hhu.bsinfo.dxgraphloader.loader.data.LongArray;
import de.hhu.bsinfo.dxgraphloader.loader.data.PeerIdsArray;
import de.hhu.bsinfo.dxgraphloader.util.Barrier;
import de.hhu.bsinfo.dxgraphloader.util.IDUtils;
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

public final class LoadChunkManagerJob extends AbstractJob {
    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());

    private int m_workerCount;
    private int m_cycle;

    private long m_arrayID;
    private long m_graph;

    private String m_classpath;

    private ConcurrentHashMap<String, Long> m_vertexMap = new ConcurrentHashMap<String, Long>();

    @SuppressWarnings("unused")
    public LoadChunkManagerJob() {
        super();
    }

    @SuppressWarnings("unused")
    public LoadChunkManagerJob(final long p_graphID, final long p_longArray,
            final String p_graphLoader, final int p_workerCount, int p_cycle) {
        m_arrayID = p_longArray;
        m_classpath = p_graphLoader;
        m_graph = p_graphID;
        m_workerCount = p_workerCount;
        m_cycle = p_cycle;
    }

    @Override
    public void execute() {
        LOGGER.info("Started '%s' from '%s'!", LoadChunkManagerJob.class.getSimpleName(),
                GraphLoaderApp.class.getSimpleName());

        final GraphLoaderContext context = new GraphLoaderContext(getService(BootService.class),
                getService(ChunkService.class), getService(ChunkLocalService.class), getService(JobService.class),
                getService(NameserviceService.class), getService(SynchronizationService.class));

        GraphObject graphObject = new GraphObject(m_graph);
        context.getChunkService().get().get(graphObject);

        LongArray longArray = new LongArray(m_arrayID);
        context.getChunkLocalService().getLocal().get(longArray);
        Queue<Long> chunkList = new ConcurrentLinkedQueue<>();

        int cycleBarrier = Barrier.getBarrier(GraphLoader.CYCLE_LOCK, context.getNameserviceService());

        long[] chunkIDs = longArray.getIds();

        for (long id : chunkIDs) {
            chunkList.add(id);
        }

        context.getChunkService().remove().remove(longArray);
        {
            if(m_cycle == 0) {
                loadVertices(context, chunkList, graphObject);
            }else {
                loadEdges(context, chunkList, graphObject);
            }
        }
        context.getSynchronizationService().barrierSignOn(cycleBarrier, ChunkID.INVALID_ID, true);
    }

    private void loadVertices(GraphLoaderContext p_context, Queue<Long> p_chunkList, GraphObject p_graph) {
        List<LoadChunkLocalJob> jobList = new ArrayList<>();
        int loadBarrier = Barrier.getBarrier(GraphLoader.LOAD_LOCK, p_context.getNameserviceService());
        Lock lock = new ReentrantLock(true);

        LOGGER.info("Extracting and Creating Local Vertices!");
        ArrayList<Set<String>> remoteKeySet = new ArrayList<>();

        for (int j = 0; j < p_context.getBootService().getOnlinePeerNodeIDs().size(); j++) {
            remoteKeySet.add(Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>()));
        }

        for (int i = 0; i < m_workerCount - 1; i++) {

            LoadChunkLocalJob abstractJob = new LoadChunkLocalJob(p_chunkList, m_classpath, lock, p_graph,
                    m_vertexMap, remoteKeySet);
            jobList.add(abstractJob);
            p_context.getJobService().pushJob(abstractJob);
        }

        LoadChunkLocalJob localJob = new LoadChunkLocalJob(p_chunkList, m_classpath, lock, p_graph,
                m_vertexMap, remoteKeySet);
        localJob.setServicesForLocal(p_context);
        jobList.add(localJob);
        localJob.execute();

        while (!areAllJobsFinished(jobList)) {
            try {

                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        jobList.clear();
        LOGGER.info("Storing Remote Keys!");
        PeerIdsArray finder = new PeerIdsArray(p_context.getBootService().getOnlinePeerNodeIDs().size());

        for (int i = 0; i < p_context.getBootService().getOnlinePeerNodeIDs().size(); i++) {
            if (p_context.getBootService().getOnlinePeerNodeIDs().get(i) == p_context.getBootService().getNodeID()) {
                continue;
            }

            Set<String> set = remoteKeySet.get(i);
            long[] ids = new long[set.size() / 5324];
            Arrays.fill(ids, ChunkID.INVALID_ID);
            LOGGER.info("Keys for Peer: '%s'",
                    IDUtils.shortToHexString(p_context.getBootService().getOnlinePeerNodeIDs().get(i)));
            int pos = 0;
            HashSet<String> smallSet = new HashSet<>();
            Iterator<String> iterator = set.iterator();

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
        PeerIdsArray[] finders = new PeerIdsArray[finderIDs.length];

        for (int i = 0; i < finderIDs.length; i++) {

            finders[i] = new PeerIdsArray(finderIDs[i]);
            p_context.getChunkService().get().get(finders);

            for (long l : Objects.requireNonNull(
                    finders[i].getArray(p_graph.getPeerPos(p_context.getBootService().getNodeID())))) {
                if (l == ChunkID.INVALID_ID) {

                    continue;
                }

                LOGGER.debug("Stored '%s'", Long.toHexString(l));
                KeyArray keyArray = new KeyArray(l);
                p_context.getChunkService().get().get(keyArray);

                for (String s : keyArray.getKeys()) {

                    Long value = m_vertexMap.putIfAbsent(s, ChunkID.INVALID_ID);

                    if (value == null) {

                        Vertex v = new Vertex();
                        p_context.getChunkLocalService().createLocal().create(v);
                        m_vertexMap.replace(s, v.getID());
                    }
                }
                p_context.getChunkService().remove().remove(keyArray);
            }
        }
        p_context.getChunkService().remove().remove(finder);

        m_vertexMap.values().forEach(value -> p_context.getChunkService().put().put(new Vertex()));

        LOGGER.info("'%s' holds '%s' Vertices",
                IDUtils.shortToHexString(p_context.getBootService().getNodeID()),
                m_vertexMap.values().size());
    }

    private void loadEdges(GraphLoaderContext p_context, Queue<Long> p_chunkList, GraphObject p_graph) {
        LOGGER.info("Loaded edges!");
    }

    private static boolean areAllJobsFinished(List<LoadChunkLocalJob> p_job) {

        for (LoadChunkLocalJob job : p_job) {
            if (!job.isFinished()) {

                return false;
            }
        }

        return true;
    }

    @Override
    public void importObject(final Importer p_importer) {

        super.importObject(p_importer);
        m_arrayID = p_importer.readLong(m_arrayID);
        m_classpath = p_importer.readString(m_classpath);
        m_workerCount = p_importer.readInt(m_workerCount);
        m_graph = p_importer.readLong(m_graph);
        m_cycle = p_importer.readInt(m_cycle);

    }

    @Override
    public void exportObject(final Exporter p_exporter) {

        super.exportObject(p_exporter);
        p_exporter.writeLong(m_arrayID);
        p_exporter.writeString(m_classpath);
        p_exporter.writeInt(m_workerCount);
        p_exporter.writeLong(m_graph);
        p_exporter.writeInt(m_cycle);

    }

    @Override
    public int sizeofObject() {

        return super.sizeofObject() + Long.BYTES + ObjectSizeUtil.sizeofString(m_classpath) + Integer.BYTES +
                Long.BYTES + Integer.BYTES;

    }
}