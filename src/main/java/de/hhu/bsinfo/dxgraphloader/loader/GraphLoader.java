package de.hhu.bsinfo.dxgraphloader.loader;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.loader.data.ChunkIDArray;
import de.hhu.bsinfo.dxgraphloader.loader.data.DistributedObjectTable;
import de.hhu.bsinfo.dxgraphloader.loader.data.FileChunk;
import de.hhu.bsinfo.dxgraphloader.loader.formats.FileChunkCreator;
import de.hhu.bsinfo.dxgraphloader.loader.formats.GraphFormat;
import de.hhu.bsinfo.dxgraphloader.loader.formats.GraphFormatReader;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.job.JobService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class GraphLoader {


    //All Services
    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());
    private final BootService bootService;
    private final JobService jobService;
    private final ChunkService chunkService;
    public final SupportedFormats supportedFormats = new SupportedFormats();


    //Setting up services
    public GraphLoader(final BootService bootService, final JobService jobService, final ChunkService chunkService) {
        this.bootService = bootService;
        this.chunkService = chunkService;
        this.jobService = jobService;
    }


    public DistributedObjectTable loadFormat(String format, String[] file_paths) {
        return loadFormat(format, file_paths, 1);
    }

    public DistributedObjectTable loadFormat(String format, String[] file_paths, int workerCount) {
        //parse while reading excludes the reading peer!
        List<Short> peers;
        peers = bootService.getOnlinePeerNodeIDs();

        //the distrubuted objecttable is a refererence to all vertices craeted ion the peers which are store in maps
        DistributedObjectTable distributedObjectTable = new DistributedObjectTable(peers, chunkService);

        //available peers
        for (short p : peers) {
            LOGGER.debug(Integer.toHexString(p).substring(4).toUpperCase());
        }

        //graphformat if supported != null
        GraphFormat graphFormat = supportedFormats.getFormat(format, file_paths);
        FileChunkCreator chunkCreator;

        if (graphFormat != null) {

            //some formats need multiple cycles to resolve nodes and edges
            //(typically first node creation and then create edges between)
            for (short cycle = 0; cycle < graphFormat.CYCLES; cycle++) {
                if (cycle > 0) continue;
                LOGGER.info("Started cycle '%s'!", cycle);

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

                        LOGGER.debug("Chunk ID: " + Long.toHexString(fileChunk.getID()));
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
                ChunkIDArray[] chunkIDArray = new ChunkIDArray[peers.size()];
                for (int i = 0; i < peers.size(); i++) {
                    chunkIDArray[i] = new ChunkIDArray(peerChunkList.get(i).toArray(new Long[0]));
                    chunkService.create().create(peers.get(i), chunkIDArray[i]);
                    chunkService.put().put(chunkIDArray[i]);

                    //start jobs (local and remote)
                    startJobsOnRemote(peers.get(i), chunkIDArray[i].getID(), graphFormat.getGraphFormatReader());
                }
                jobService.waitForLocalJobsToFinish();
            }
        }
        return distributedObjectTable;
    }

    private void startJobsOnRemote(short p, long chunkIDArrayID, GraphFormatReader graphFormatReader) {

        LOGGER.info("Pushing loader " + LoadChunkManagerJob.class.getSimpleName() + " to "
                + Integer.toHexString(p).substring(4).toUpperCase() + "!");

        AbstractJob abstractJob = jobService.createJobInstance(LoadChunkManagerJob.class.getCanonicalName(),
                chunkIDArrayID, graphFormatReader.getClass().getCanonicalName(), 4);

        if (bootService.getNodeID() != p) {
            jobService.pushJobRemote(abstractJob, p);
        } else {
            jobService.pushJob(abstractJob);
        }
    }
}

