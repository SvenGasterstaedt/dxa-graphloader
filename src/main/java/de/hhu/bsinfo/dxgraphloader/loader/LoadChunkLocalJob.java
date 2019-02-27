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

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.loader.data.GraphObject;
import de.hhu.bsinfo.dxgraphloader.loader.data.FileChunk;
import de.hhu.bsinfo.dxgraphloader.loader.formats.GraphFormatReader;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public final class LoadChunkLocalJob extends AbstractJob {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());
    private static final Class[] graphFormatConstructor = new Class[]{GraphObject.class, ConcurrentHashMap.class, ArrayList.class, ChunkLocalService.class, BootService.class};

    private Queue<Long> queue;
    private String classPath;
    private GraphObject graphObject;

    private ChunkService chunkService;
    private ChunkLocalService chunkLocalService;
    private BootService bootService;
    private NameserviceService nameserviceService;
    private SynchronizationService synchronizationService;
    private Lock queueLock;

    private ConcurrentHashMap<String, Long> localKeys;
    private ArrayList<Set<String>> remoteKeys;

    boolean finished = false;


    public LoadChunkLocalJob(final Queue<Long> queue, final String classPath, final Lock queueLock, final GraphObject graphObject, ConcurrentHashMap<String, Long> localKeys, ArrayList<Set<String>> remoteKeys) {
        this.queue = queue;
        this.classPath = classPath;
        this.queueLock = queueLock;
        this.graphObject = graphObject;
        this.remoteKeys = remoteKeys;
        this.localKeys = localKeys;
    }

    public void setServicesForLocal(final BootService bootService, final ChunkService chunkService, final ChunkLocalService chunkLocalService, final NameserviceService nameserviceService, final SynchronizationService synchronizationService) {
        this.bootService = bootService;
        this.chunkService = chunkService;
        this.chunkLocalService = chunkLocalService;
        this.nameserviceService = nameserviceService;
        this.synchronizationService = synchronizationService;
    }

    @Override
    public void execute() {
        if (chunkService == null || bootService == null || chunkLocalService == null || nameserviceService == null || synchronizationService == null) {
            chunkLocalService = getService(ChunkLocalService.class);
            bootService = getService(BootService.class);
            chunkService = getService(ChunkService.class);
            nameserviceService = getService(NameserviceService.class);
            synchronizationService = getService(SynchronizationService.class);
        }
        if (chunkService == null || bootService == null || chunkLocalService == null || nameserviceService == null || synchronizationService == null) {
            LOGGER.error("Started Job on local scope without setting Services");
            return;
        }

        int stepBarrier = BarrierID.INVALID_ID;
        while (stepBarrier == BarrierID.INVALID_ID) {
            stepBarrier = (int) nameserviceService.getChunkID(GraphLoader.LOAD_LOCK, 1000);
        }

        while (queue.size() > 0) {
            long chunkID;
            boolean isLockAcquired = false;
            try {
                isLockAcquired = queueLock.tryLock(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if(isLockAcquired) {
                try {
                    chunkID = queue.remove();
                } finally {
                    queueLock.unlock();
                }
                FileChunk fileChunk = new FileChunk(chunkID);
                chunkLocalService.getLocal().get(fileChunk);

                try {

                    GraphFormatReader graphFormatReader = (GraphFormatReader) Class.forName(classPath)
                            .getConstructor(graphFormatConstructor)
                            .newInstance(graphObject, localKeys, remoteKeys, chunkLocalService, bootService);

                    graphFormatReader.readVertices(fileChunk.getContents());

                    LOGGER.debug(Long.toHexString(chunkID) + " loaded!");

                } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException | InstantiationException | IllegalAccessException e) {
                    e.printStackTrace();
                    break;
                }

                chunkService.remove().remove(fileChunk);
            }
        }
        LOGGER.debug("Finished Load Chunk!");
        finished = true;
    }

    public boolean isFinished() {
        return finished;
    }
}
