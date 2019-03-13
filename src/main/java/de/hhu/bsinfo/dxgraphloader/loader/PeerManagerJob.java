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

        //barrier to sync cycles of loading
        int cycleBarrier = Barrier.getBarrier(GraphLoader.CYCLE_LOCK, context.getNameserviceService());

        //Function to execute depend on which cycle we are
        if (m_cycle == 0) {

            VertexLoader vertexLoader = new VertexLoader(context, chunkList, graph, m_classpath);
            vertexLoader.load();

        } else if (m_cycle == 1) {
            EdgeLoader edgeLoader = new EdgeLoader(context, chunkList, graph,m_classpath);
            edgeLoader.load();
        }
        GraphLoader.logMem();

        //release barrier and end the cycle - wait for all other peers to finish
        context.getSynchronizationService().barrierSignOn(cycleBarrier, ChunkID.INVALID_ID, true);
    }


    private static boolean areAllJobsFinished(List<LoadChunkJob> p_job) {
        for (LoadChunkJob job : p_job) {
            if (!job.isFinished()) {

                return false;
            }
        }

        return true;
    }

    static void waitForJobsToFinish(List<LoadChunkJob> p_job) {
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