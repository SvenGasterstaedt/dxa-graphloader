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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.loader.data.FileChunk;
import de.hhu.bsinfo.dxgraphloader.loader.data.Graph;
import de.hhu.bsinfo.dxgraphloader.loader.formats.AbstractGraphFormatReader;
import de.hhu.bsinfo.dxgraphloader.util.Tuple;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;

public final class LoadChunkJob extends AbstractJob {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());
    private static final Class[] CLASSES = new Class[] {
            Graph.class,
            ConcurrentHashMap.class,
            ConcurrentHashMap.class,
            ArrayList.class,
            ChunkLocalService.class,
            BootService.class};

    private Queue<Long> m_queue;
    private String m_classpath;
    private Graph m_graph;
    private int m_cycle;

    private PeerContext m_context;
    private Lock m_lock;

    private ConcurrentHashMap<Long, Long> m_localKeys;
    private ConcurrentHashMap<Tuple<Long, Long>, Long> m_remoteEdge;
    private ArrayList<Set<Long>> m_remoteKeys;

    private boolean m_finished;

    public LoadChunkJob(final Queue<Long> p_queue, final String p_classpath, final Lock p_lock,
            final Graph p_graph,
            final ConcurrentHashMap<Long, Long> p_localKeys, final ArrayList<Set<Long>> p_remoteVertices, int p_cycle) {
        m_graph = p_graph;
        m_remoteKeys = p_remoteVertices;
        m_localKeys = p_localKeys;
        m_classpath = p_classpath;
        m_queue = p_queue;
        m_lock = p_lock;
        m_cycle = p_cycle;
    }

    public LoadChunkJob(final Queue<Long> p_queue, final String p_classpath, final Lock p_lock,
            final Graph p_graph, final ConcurrentHashMap<Tuple<Long, Long>, Long> p_remoteEdge, int p_cycle) {
        m_graph = p_graph;
        m_remoteEdge = p_remoteEdge;
        m_classpath = p_classpath;
        m_queue = p_queue;
        m_lock = p_lock;
        m_cycle = p_cycle;
    }

    void setServicesForLocal(final PeerContext p_context) {
        m_context = p_context;
    }

    @Override
    public void execute() {
        if (m_context == null) {
            m_context = new PeerContext(getService(BootService.class),
                    getService(ChunkService.class),
                    getService(ChunkLocalService.class),
                    getService(JobService.class),
                    getService(NameserviceService.class),
                    getService(SynchronizationService.class));
        }

        while (!m_queue.isEmpty()) {

            long chunkID;
            boolean isLockAcquired = false;

            try {
                isLockAcquired = m_lock.tryLock(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (isLockAcquired) {
                try {
                    chunkID = m_queue.remove();
                } catch (NoSuchElementException e) {
                    continue;
                } finally {
                    m_lock.unlock();
                }
                FileChunk fileChunk = new FileChunk(chunkID);
                m_context.getChunkLocalService().getLocal().get(fileChunk);

                try {
                    AbstractGraphFormatReader graphFormatReader = null;
                    switch (m_cycle) {
                        case 0:
                            graphFormatReader = (AbstractGraphFormatReader) Class.forName(
                                    m_classpath)
                                    .getConstructor(CLASSES)
                                    .newInstance(m_graph, m_localKeys, m_remoteEdge, m_remoteKeys,
                                            m_context.getChunkLocalService(),
                                            m_context.getBootService());
                            graphFormatReader.readVertices(fileChunk.getContents());
                            break;
                        case 1:
                            graphFormatReader = (AbstractGraphFormatReader) Class.forName(
                                    m_classpath)
                                    .getConstructor(CLASSES)
                                    .newInstance(m_graph, m_localKeys, m_remoteEdge, m_remoteKeys,
                                            m_context.getChunkLocalService(),
                                            m_context.getBootService());
                            graphFormatReader.readEdges(fileChunk.getContents());
                            break;
                        default:
                            //nothing :-)
                            break;
                    }
                    LOGGER.debug("'%s' loaded!", Long.toHexString(chunkID));

                } catch (ClassNotFoundException | IllegalAccessException | InstantiationException
                        | InvocationTargetException | NoSuchMethodException e) {
                    e.printStackTrace();
                    break;
                }

                m_context.getChunkService().remove().remove(fileChunk);
            }
        }
        LOGGER.debug("Finished Load Chunk!");
        m_finished = true;
    }

    boolean isFinished() {
        return m_finished;
    }
}
