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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * "GraphLoaderApp"
 **/

@SuppressWarnings("Duplicates")
public class GraphLoaderApp extends AbstractApplication {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());
    private List<Short> peers;
    private List<Long> filechunks_ids;
    private ChunkService chunkService;
    private JobService jobService;
    private ApplicationService applicationService;
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
        applicationService = getService(ApplicationService.class);
        List<Short> peers = bootService.getOnlinePeerNodeIDs().stream().filter(s -> !s.equals(bootService.getNodeID())).collect(Collectors.toList());


        JobRegistration jobRegistration = new JobRegistration(bootService, applicationService);
        if (!jobRegistration.registerJob(RemoteJob.class)) {
            LOGGER.error("Not all jobs registered!");
            return;
        }

        if (p_args.length < 2) {
            LOGGER.error("Parameters required! <format> <files>\nTerminated!");
            return;
        }

        String format = p_args[0].toUpperCase();
        String[] file_paths = Arrays.copyOfRange(p_args, 1, p_args.length);
        try {
            this.filechunks_ids = new ArrayList<>();
            chunkService = getService(ChunkService.class);
            jobService = getService(JobService.class);

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
            if (graphFormat != null) {
                chunkCreator = graphFormat.getFileChunkCreator();
                while (chunkCreator.hasRemaining()) {
                    for (short p : peers) {
                        long c_id = ChunkID.INVALID_ID;
                        FileChunk fileChunk = chunkCreator.getNextChunk();
                        if (chunkService.create().create(p, fileChunk) > 0) {
                            chunkService.put().put(fileChunk);
                            filechunks_ids.add(fileChunk.getID());
                        }
                        if (!chunkCreator.hasRemaining()) {
                            break;
                        }
                    }
                }
                for (short p : peers) {
                    LOGGER.debug("Pushing job "+ RemoteJob.class.getSimpleName() +" to " + Integer.toHexString(p).substring(4).toUpperCase()+ "!");
                    RemoteJob remoteJob = new RemoteJob();
                    jobService.pushJobRemote(remoteJob, p);
                }
                //filechunks_ids.stream().forEach(LOGGER::debug);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("GraphLoader terminated!");
        }

    }


    //registers jobs on all peers, duo to the fact that dxram isn't keepable of doing this yet.


    @Override
    public void signalShutdown() {
    }
}
