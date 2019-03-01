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
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.loader.data.FileChunk;
import de.hhu.bsinfo.dxgraphloader.loader.data.GraphObject;
import de.hhu.bsinfo.dxgraphloader.loader.formats.AbstractGraphFormatReader;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;

public final class LoadChunkLocalJob extends AbstractJob {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());
    private static final Class[] GRAPHFORMATCONSTRUCTOR = new Class[] {GraphObject.class, ConcurrentHashMap.class,
            ArrayList.class, ChunkLocalService.class, BootService.class};

    private Queue<Long> m_queue;
    private String m_classpath;
    private GraphObject m_graph;

    private GraphLoaderContext m_context;
    private Lock m_lock;

    private ConcurrentHashMap<String, Long> m_localKeys;
    private ArrayList<Set<String>> m_remoteKeys;

    private boolean m_finished;

    @SuppressWarnings({"unused", "WeakerAccess"})
    public LoadChunkLocalJob(final Queue<Long> p_queue, final String p_classpath, final Lock p_lock,
            final GraphObject p_graph,
            final ConcurrentHashMap<String, Long> p_localKeys, final ArrayList<Set<String>> p_remoteKeys) {

        m_context = new GraphLoaderContext(getService(BootService.class), getService(ChunkService.class),
                getService(ChunkLocalService.class), getService(JobService.class), getService(NameserviceService.class),
                getService(SynchronizationService.class));

        m_graph = p_graph;
        m_remoteKeys = p_remoteKeys;
        m_localKeys = p_localKeys;

        m_classpath = p_classpath;

        m_queue = p_queue;
        m_lock = p_lock;

    }

    void setServicesForLocal(final GraphLoaderContext p_context) {
        m_context = p_context;
    }

    @Override
    public void execute() {
        if (m_context == null) {
            LOGGER.error("Started Job on local scope without setting Services");
            return;
        }

        int stepBarrier = BarrierID.INVALID_ID;
        while (stepBarrier == BarrierID.INVALID_ID) {
            stepBarrier = (int) m_context.getNameserviceService().getChunkID(GraphLoader.LOAD_LOCK, 1000);
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
                } finally {
                    m_lock.unlock();
                }
                FileChunk fileChunk = new FileChunk(chunkID);
                m_context.getChunkLocalService().getLocal().get(fileChunk);

                try {

                    AbstractGraphFormatReader graphFormatReader = (AbstractGraphFormatReader) Class.forName(m_classpath)
                            .getConstructor(GRAPHFORMATCONSTRUCTOR)
                            .newInstance(m_graph, m_localKeys, m_remoteKeys, m_context.getChunkLocalService(),
                                    m_context.getBootService());

                    graphFormatReader.readVertices(fileChunk.getContents());

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

    @SuppressWarnings("WeakerAccess")
    public boolean isFinished() {
        return m_finished;
    }
}
