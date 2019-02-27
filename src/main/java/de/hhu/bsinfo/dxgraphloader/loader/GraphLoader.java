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
import de.hhu.bsinfo.dxgraphloader.loader.data.LongArray;
import de.hhu.bsinfo.dxgraphloader.loader.data.FileChunk;
import de.hhu.bsinfo.dxgraphloader.loader.data.GraphObject;
import de.hhu.bsinfo.dxgraphloader.loader.formats.FileChunkCreator;
import de.hhu.bsinfo.dxgraphloader.loader.formats.GraphFormat;
import de.hhu.bsinfo.dxgraphloader.loader.formats.GraphFormatReader;
import de.hhu.bsinfo.dxgraphloader.util.IDUtils;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * <h1>GraphLoader</h1>
 * The GraphLoader program implements a library/tool for
 * distributed graph loading in DxRam.
 * Own custom formats can be added and loaded!
 *
 * @author Sven Gasterstaedt
 * @version 1.0
 * @since 2019-03-15
 */

public final class GraphLoader {


    public final static String CYCLE_LOCK = "GRL1";
    public final static String LOAD_LOCK = "GRL2";


    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());
    private final BootService bootService;
    private final JobService jobService;
    private final ChunkService chunkService;
    private final NameserviceService nameserviceService;
    private final SynchronizationService synchronizationService;

    public final SupportedFormats supportedFormats = new SupportedFormats();

    private GraphObject graphObject;


    /**
     * This method is used to set up the GraphLoader
     * and adds all needed services
     *
     * @param bootService            DxRam BootService from the current peer
     * @param jobService             DxRam JobService from the current peer
     * @param chunkService           DxRam ChunkService from the current peer
     * @param nameserviceService     DxRam NameserviceService from the current peer
     * @param synchronizationService DxRam SynchronizationService from the current peer
     */
    public GraphLoader(final BootService bootService, final JobService jobService, final ChunkService chunkService, NameserviceService nameserviceService, SynchronizationService synchronizationService) {
        this.bootService = bootService;
        this.chunkService = chunkService;
        this.jobService = jobService;
        this.nameserviceService = nameserviceService;
        this.synchronizationService = synchronizationService;
    }

    /**
     * This method is used to set up the GraphLoader
     * and adds all needed services
     * With the default value for the workers.
     *
     * @param format     Format string matching one of the supported formats - see this.supportedFormat.
     * @param file_paths Paths to the files to be loaded / For multiple file loading rerun this function.
     * @return A Graph object
     */
    public GraphObject loadFormat(String format, String[] file_paths) {
        return loadFormat(format, file_paths, 2);
    }

    /**
     * This method is used to set up the GraphLoader
     * and adds all needed services
     *
     * @param format      Format string matching one of the supported formats - see this.supportedFormat.
     * @param file_paths  Paths to the files to be loaded / For multiple file loading rerun this function.
     * @param workerCount Sets the value how much jobs, will be created on each peer. (Threads).
     *                    This amount can't exceed the value in the configuration of the JobComponent
     * @return A Graph object
     */
    public GraphObject loadFormat(String format, String[] file_paths, int workerCount) {
        //parse while reading excludes the reading peer!
        List<Short> peers;
        peers = bootService.getOnlinePeerNodeIDs();

        //the distrubuted objecttable is a refererence to all vertices craeted ion the peers which are store in maps
        graphObject = new GraphObject(peers, chunkService);
        chunkService.create().create(bootService.getNodeID(), graphObject);
        chunkService.put().put(graphObject);

        //graphformat if supported != null
        GraphFormat graphFormat = supportedFormats.getFormat(format, file_paths);
        FileChunkCreator chunkCreator;

        if (graphFormat != null) {

            //some formats need multiple cycles to resolve nodes and edges
            //(typically first node creation and then create edges between)
            for (short cycle = 0; cycle < graphFormat.CYCLES; cycle++) {
                if (cycle > 0) continue;
                LOGGER.info("Started cycle '%s' from '%s'!", cycle + 1, graphFormat.CYCLES);

                chunkCreator = graphFormat.getFileChunkCreator(cycle);

                List<List<Long>> peerChunkList = new ArrayList<>();

                for (short p : peers) {
                    peerChunkList.add(new ArrayList<Long>());
                }

                //just chunk creation and distrubution - nothing with formats
                while (chunkCreator.hasRemaining()) {
                    for (short p : peers) {
                        FileChunk fileChunk = chunkCreator.getNextChunk();
                        if (chunkService.create().create(p, fileChunk) != 1) {
                            LOGGER.warn("Creation failed, 2nd try!");
                            if (chunkService.create().create(p, fileChunk) != 1) {
                                LOGGER.error("Failed again!");
                                LOGGER.error("Probably other peers busy or not enough memory!");
                                return null;
                            }
                        }

                        //LOGGER.debug("Chunk ID: " + Long.toHexString(fileChunk.getID()));
                        if (!chunkService.put().put(fileChunk)) {
                            LOGGER.warn("Putting failed, 2nd try!");
                            if (!chunkService.put().put(fileChunk)) {
                                LOGGER.error("Failed again!");
                                LOGGER.error("Probably other peers busy or not enough memory!");
                                return null;
                            }
                        }

                        peerChunkList.get(peers.indexOf(p)).add(fileChunk.getID());

                        if (!chunkCreator.hasRemaining()) {
                            break;
                        }
                    }
                }
                //push chunk ids to peers
                int cycleBarrier = synchronizationService.barrierAllocate(peers.size());
                synchronizationService.barrierGetStatus(cycleBarrier);
                nameserviceService.register(cycleBarrier, CYCLE_LOCK);
                int loadBarrier = synchronizationService.barrierAllocate(peers.size());
                synchronizationService.barrierGetStatus(loadBarrier);
                nameserviceService.register(loadBarrier, LOAD_LOCK);

                {
                    LongArray[] longArray = new LongArray[peers.size()];
                    for (int i = 0; i < peers.size(); i++) {
                        longArray[i] = new LongArray(peerChunkList.get(i).toArray(new Long[0]));
                        chunkService.create().create(peers.get(i), longArray[i]);
                        chunkService.put().put(longArray[i]);

                        //start jobs (local and remote)
                        startJobsOnRemote(peers.get(i), longArray[i].getID(), graphFormat.getGraphFormatReader(), workerCount);
                    }
                }
                jobService.waitForLocalJobsToFinish();
                synchronizationService.barrierFree(cycleBarrier);
                synchronizationService.barrierFree(loadBarrier);
            }
        }
        return graphObject;
    }

    private void startJobsOnRemote(short p, long chunkIDArrayID, Class<? extends GraphFormatReader> graphFormatReader, int workerCount) {

        LOGGER.info("Pushing loader " + LoadChunkManagerJob.class.getSimpleName() + " to "
                + IDUtils.shortToHexString(p) + "!");
        AbstractJob abstractJob = jobService.createJobInstance(LoadChunkManagerJob.class.getCanonicalName(), graphObject.getID(),
                chunkIDArrayID, graphFormatReader.getCanonicalName(), workerCount);

        if (bootService.getNodeID() != p) {
            jobService.pushJobRemote(abstractJob, p);
        } else {
            jobService.pushJob(abstractJob);
        }
    }
}

