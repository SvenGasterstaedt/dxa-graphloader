package de.hhu.bsinfo.dxapp;

import de.hhu.bsinfo.dxapp.data.FileChunk;
import de.hhu.bsinfo.dxapp.formats.GraphFormat;
import de.hhu.bsinfo.dxapp.formats.SupportedFormats;
import de.hhu.bsinfo.dxapp.formats.split.FileChunkCreator;
import de.hhu.bsinfo.dxapp.job.JobRegistration;
import de.hhu.bsinfo.dxapp.job.RemoteJob;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.app.ApplicationService;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import de.hhu.bsinfo.dxram.job.JobService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.rmi.runtime.Log;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * "GraphLoaderApp"
 **/

@SuppressWarnings("Duplicates")
public class GraphLoaderApp extends AbstractApplication {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());
    private BootService bootService;

    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return BuildConfig.DXRAM_VERSION;
    }

    @Override
    public String getApplicationName() {
        return "GraphLoaderApp";
    }

    @Override
    public void main(final String[] p_args) {
        bootService = getService(BootService.class);
        ApplicationService applicationService = getService(ApplicationService.class);
        List<Short> peers = bootService.getOnlinePeerNodeIDs().stream().filter(s -> !s.equals(bootService.getNodeID())).collect(Collectors.toList());


        JobRegistration jobRegistration = new JobRegistration(bootService, applicationService);
        if (!jobRegistration.registerJob(RemoteJob.class)) {
            LOGGER.error("Not all jobs registered!");
            //return;
        }

        if (p_args.length < 2) {
            LOGGER.error("Parameters required! <format> <files>\nTerminated!");
            return;
        }

        String format = p_args[0].toUpperCase();
        String[] file_paths = Arrays.copyOfRange(p_args, 1, p_args.length);
        try {
            ChunkService chunkService = getService(ChunkService.class);
            JobService jobService = getService(JobService.class);

            if (!SupportedFormats.isSupported(format)) {
                LOGGER.error(format + " is no not supported!");
                LOGGER.info("List of supported formats:");
                for (String f : SupportedFormats.supportedFormats()) {
                    LOGGER.info(f);
                }
                return;
            }

            for (String file : file_paths) {
                if (!Files.isRegularFile(Paths.get(file))) {
                    LOGGER.error(file + " is no regular file!");
                    return;
                }
            }


            GraphFormat graphFormat = SupportedFormats.getFormat(format, file_paths);
            FileChunkCreator chunkCreator;
            boolean send =false;

            if (graphFormat != null) {
                chunkCreator = graphFormat.getFileChunkCreator();
                FileChunk[] prevFileChunks = new FileChunk[peers.size()];

                while (chunkCreator.hasRemaining()) {
                    for (short p : peers) {
                        FileChunk fileChunk = chunkCreator.getNextChunk();
                        if(chunkService.create().create(p, fileChunk)!=1){
                            LOGGER.warn("Creation failed, 2nd try!");
                            if(chunkService.create().create(p, fileChunk)!=1){
                                LOGGER.error("Failed again!");
                                return;
                            }
                        }

                        if (prevFileChunks[peers.indexOf(p)] != null) {
                            LOGGER.debug("prev chunk ID: " + Long.toHexString(fileChunk.getID()));
                            prevFileChunks[peers.indexOf(p)].setNextID(fileChunk.getID());
                            chunkService.put().put(prevFileChunks[peers.indexOf(p)]);

                            if(!send){
                                submitJob(peers, prevFileChunks, graphFormat, jobService);
                                send=true;
                            }
                        }

                        prevFileChunks[peers.indexOf(p)] = fileChunk;

                        if (!chunkCreator.hasRemaining()) {
                            break;
                        }
                    }
                }
                for(short p:peers) {
                    if(prevFileChunks[peers.indexOf(p)]!=null) {
                        prevFileChunks[peers.indexOf(p)].setHasNext(false);
                        chunkService.put().put(prevFileChunks[peers.indexOf(p)]);
                    }
                }
            }
        } catch (
                Exception e) {
            e.printStackTrace();
            LOGGER.error("GraphLoader terminated!");
        }
    }


    //registers jobs on all peers, duo to the fact that dxram isn't keepable of doing this yet.


    @Override
    public void signalShutdown() {
    }

    private void submitJob(List<Short> peers, FileChunk[] chunkIds, GraphFormat graphFormat, JobService jobService) {
        for (short p : peers) {
            LOGGER.debug("Pushing job " + RemoteJob.class.getSimpleName() + " to " + Integer.toHexString(p).substring(4).toUpperCase() + "!");
            LOGGER.debug("Chunk ID: " + Long.toHexString(chunkIds[peers.indexOf(p)].getID()));
            RemoteJob remoteJob = new RemoteJob(chunkIds[peers.indexOf(p)].getID(), graphFormat.getGraphFormatReader().getClass());
            jobService.pushJobRemote(remoteJob, p);
        }
    }
}
