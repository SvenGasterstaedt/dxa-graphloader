package de.hhu.bsinfo.dxgraphloader.app;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.app.data.ChunkIDArray;
import de.hhu.bsinfo.dxgraphloader.app.data.FileChunk;
import de.hhu.bsinfo.dxgraphloader.formats.GraphFormat;
import de.hhu.bsinfo.dxgraphloader.formats.SupportedFormats;
import de.hhu.bsinfo.dxgraphloader.formats.parsers.GraphFormatReader;
import de.hhu.bsinfo.dxgraphloader.formats.splitter.FileChunkCreator;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.job.JobService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class GraphLoader {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());
    private BootService bootService;
    private JobService jobService;
    private ChunkService chunkService;
    public SupportedFormats supportedFormats = new SupportedFormats();


    public GraphLoader(BootService bootService, JobService jobService, ChunkService chunkService) {
        this.bootService = bootService;
        this.chunkService = chunkService;
        this.jobService = jobService;
    }

    public boolean loadFormat(String format, String[] file_paths) {
        List<Short> peers = bootService.getOnlinePeerNodeIDs();//.stream().filter(s -> !s.equals(bootService.getNodeID())).collect(Collectors.toList());
        for (short p : peers) {
            LOGGER.debug(Integer.toHexString(p).substring(4).toUpperCase());
        }

        GraphFormat graphFormat = supportedFormats.getFormat(format, file_paths);
        FileChunkCreator chunkCreator;

        if (graphFormat != null) {
            chunkCreator = graphFormat.getFileChunkCreator();

            ChunkIDArray[] chunkIDArrays = new ChunkIDArray[peers.size()];
            for (int i = 0; i < peers.size(); i++) {
                chunkIDArrays[i] = new ChunkIDArray(chunkCreator.getApproxChunkAmount());
                chunkService.create().create(peers.get(i), chunkIDArrays[i]);
            }

            while (chunkCreator.hasRemaining()) {
                for (short p : peers) {
                    FileChunk fileChunk = chunkCreator.getNextChunk();
                    if (chunkService.create().create(p, fileChunk) != 1) {
                        LOGGER.warn("Creation failed, 2nd try!");
                        if (chunkService.create().create(p, fileChunk) != 1) {
                            LOGGER.error("Failed again!");
                            return false;
                        }
                    }

                    LOGGER.debug("Chunk ID: " + Long.toHexString(fileChunk.getID()));
                    if (!chunkService.put().put(fileChunk)) {
                        LOGGER.warn("Putting failed, 2nd try!");
                        if (!chunkService.put().put(fileChunk)) {
                            LOGGER.error("Failed again!");
                            return false;
                        }
                    }
                    chunkIDArrays[peers.indexOf(p)].addID(fileChunk.getID());

                    if (!chunkService.put().put(chunkIDArrays[peers.indexOf(p)])) {
                        LOGGER.warn("Putting failed, 2nd try!");
                        if (!chunkService.put().put(chunkIDArrays[peers.indexOf(p)])) {
                            LOGGER.error("Failed again!");
                            return false;
                        }
                    }

                    if (!chunkCreator.hasRemaining()) {
                        break;
                    }
                }
            }
            LOGGER.debug("Finished loading setting hasNext false");
            for (int i = 0; i < peers.size(); i++) {
                chunkIDArrays[i].setHasNext(false);
            }
            for (short p : peers) {
                startJobsOnRemote(p, chunkIDArrays[peers.indexOf(p)], graphFormat.getGraphFormatReader());
            }

            chunkService.put().put(chunkIDArrays);
        }
        jobService.waitForLocalJobsToFinish();
        return true;
    }

    private void startJobsOnRemote(short p, ChunkIDArray chunkIDArray, GraphFormatReader graphFormatReader) {
        LOGGER.info("Pushing app " + LoadChunkManagerJob.class.getSimpleName() + " to " + Integer.toHexString(p).substring(4).toUpperCase() + "!");
        AbstractJob abstractJob = jobService.createJobInstance(LoadChunkManagerJob.class.getCanonicalName(), chunkIDArray.getID(), graphFormatReader.getClass().getCanonicalName(), 4);
        if (bootService.getNodeID() != p) {
            jobService.pushJobRemote(abstractJob, p);
        } else {
            jobService.pushJob(abstractJob);
        }
    }
}

