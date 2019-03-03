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
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.loader.data.FileChunk;
import de.hhu.bsinfo.dxgraphloader.loader.data.Graph;
import de.hhu.bsinfo.dxgraphloader.loader.data.LongArray;
import de.hhu.bsinfo.dxgraphloader.loader.formats.AbstractFileChunkCreator;
import de.hhu.bsinfo.dxgraphloader.loader.formats.AbstractGraphFormatReader;
import de.hhu.bsinfo.dxgraphloader.loader.formats.GraphFormat;
import de.hhu.bsinfo.dxgraphloader.util.Barrier;
import de.hhu.bsinfo.dxgraphloader.util.IDUtils;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;

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

    static final String CYCLE_LOCK = "GRL1";

    static final String LOAD_LOCK = "GRL2";

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());
    private final BootService m_boot;
    private final JobService m_job;
    private final ChunkService m_chunk;
    private final NameserviceService m_name;
    private final SynchronizationService m_sync;

    private final SupportedFormats m_formats = new SupportedFormats();

    private Graph m_graph;

    /**
     * This method is used to set up the GraphLoader
     * and adds all needed services
     *
     * @param p_context
     *         all used Services of the current peer
     */
    public GraphLoader(final PeerContext p_context) {
        m_boot = p_context.getBootService();
        m_chunk = p_context.getChunkService();
        m_job = p_context.getJobService();
        m_name = p_context.getNameserviceService();
        m_sync = p_context.getSynchronizationService();
    }

    /**
     * This method is used to set up the GraphLoader
     * and adds all needed services
     * With the default value for the workers.
     *
     * @param p_format
     *         Format string matching one of the supported formats - see this.supportedFormat.
     * @param p_filePaths
     *         Paths to the files to be loaded / For multiple file loading rerun this function.
     * @return A Graph object
     */
    @SuppressWarnings("unused")
    public Graph loadFormat(String p_format, String[] p_filePaths) {
        return loadFormat(p_format, p_filePaths, 2);
    }

    /**
     * This method is used to set up the GraphLoader
     * and adds all needed services
     *
     * @param p_format
     *         Format string matching one of the supported formats - see this.supportedFormat.
     * @param p_filenames
     *         Paths to the files to be loaded / For multiple file loading rerun this function.
     * @param p_workerCount
     *         Sets the value how much jobs, will be created on each peer. (Threads).
     *         This amount can't exceed the value in the configuration of the JobComponent
     * @return A Graph object
     */
    public Graph loadFormat(String p_format, String[] p_filenames, int p_workerCount) {
        //parse while reading excludes the reading peer!
        List<Short> peers = m_boot.getOnlinePeerNodeIDs();

        //the distribution graph object is a reference to all vertices created ion the peers which are store in maps
        m_graph = new Graph(peers, m_chunk);
        m_chunk.create().create(m_boot.getNodeID(), m_graph);
        m_chunk.put().put(m_graph);

        //graph format if supported != null
        GraphFormat graphFormat = m_formats.getFormat(p_format, p_filenames);
        AbstractFileChunkCreator chunkCreator;

        if (graphFormat != null) {
            //some formats need multiple cycles to resolve nodes and edges
            //(typically first node creation and then create edges between)
            for (short cycleCount = 0; cycleCount < 2; cycleCount++) {

                LOGGER.info("Started cycle '%s' from '%s'!", cycleCount + 1, graphFormat.getCycles());

                chunkCreator = graphFormat.getFileChunkCreator(cycleCount);
                List<List<Long>> peerChunkList = new ArrayList<>();
                for (short ignored : peers) {
                    peerChunkList.add(new ArrayList<Long>());
                }

                //just chunk creation and distribution - nothing with formats
                while (chunkCreator.hasRemaining()) {
                    for (short p : peers) {

                        FileChunk fileChunk = chunkCreator.getNextChunk();
                        if (m_chunk.create().create(p, fileChunk) != 1) {

                            LOGGER.warn("Creation failed, 2nd try!");
                            if (m_chunk.create().create(p, fileChunk) != 1) {

                                LOGGER.error("Failed again!");
                                LOGGER.error("Probably other peers busy or not enough memory!");
                                return null;
                            }
                        }

                        LOGGER.debug("Chunk created: '%s'!", Long.toHexString(fileChunk.getID()));
                        if (!m_chunk.put().put(fileChunk)) {

                            LOGGER.warn("Putting failed, 2nd try!");
                            if (!m_chunk.put().put(fileChunk)) {

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
                int cycleBarrier = Barrier.createBarrier(CYCLE_LOCK, peers.size(), m_sync, m_name);
                int loadBarrier = Barrier.createBarrier(LOAD_LOCK, peers.size(), m_sync, m_name);

                {
                    LongArray[] longArray = new LongArray[peers.size()];
                    for (int i = 0; i < peers.size(); i++) {

                        longArray[i] = new LongArray(peerChunkList.get(i).toArray(new Long[0]));
                        //start jobs (local and remote)
                        startJobsOnRemote(peers.get(i), longArray[i], graphFormat.getGraphFormatReader(),
                                p_workerCount, cycleCount);
                    }
                }
                m_job.waitForLocalJobsToFinish();
                m_sync.barrierFree(cycleBarrier);
                m_sync.barrierFree(loadBarrier);
            }
        }
        return m_graph;
    }

    private void startJobsOnRemote(short p_peer, LongArray p_longArray,
            Class<? extends AbstractGraphFormatReader> p_formatReader,
            int p_workerCount, int p_cycle) {

        LOGGER.info("Pushing loader '%s' to '%s'!",
                PeerManagerJob.class.getSimpleName(), IDUtils.shortToHexString(p_peer));

        AbstractJob abstractJob = m_job.createJobInstance(PeerManagerJob.class.getCanonicalName(),
                m_graph.getID(),
                p_longArray, p_formatReader.getCanonicalName(), p_workerCount, p_cycle);

        if (m_boot.getNodeID() != p_peer) {
            m_job.pushJobRemote(abstractJob, p_peer);
        } else {
            m_job.pushJob(abstractJob);
        }
    }

    public SupportedFormats getFormats() {
        return m_formats;
    }

}

