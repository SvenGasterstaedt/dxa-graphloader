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
            return;
        }

        if (p_args.length < 2) {
            LOGGER.error("Parameters required! <format> <files>\nTerminated!");
            return;
        }

        String format = p_args[0].toUpperCase();
        String[] file_paths = Arrays.copyOfRange(p_args, 1, p_args.length);
        try {
            List<Long> filechunks_ids = new ArrayList<>();
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
            List<FileChunk> failed = new ArrayList<>();

            if (graphFormat != null) {

                chunkCreator = graphFormat.getFileChunkCreator();
                long[][] chunk_ids = new long[peers.size()][chunkCreator.getApproxChunkAmount() / peers.size() + 1];
                int i = 0;
                while (chunkCreator.hasRemaining()) {
                    for (short p : peers) {

                        FileChunk fileChunk = chunkCreator.getNextChunk();
                        chunkService.create().create(p, fileChunk);
                        chunkService.put().put(fileChunk);

                        if (fileChunk.isIDValid() && fileChunk.isStateOk()) {
                            chunk_ids[peers.indexOf(p)][i] = fileChunk.getID();


                        } else {

                            failed.add(fileChunk);

                        }
                        if (!chunkCreator.hasRemaining()) {

                            break;

                        }
                    }
                    i++;
                }
                for (short p : peers) {
                    LOGGER.debug("Pushing job " + RemoteJob.class.getSimpleName() + " to " + Integer.toHexString(p).substring(4).toUpperCase() + "!");
                    for (long l : chunk_ids[peers.indexOf(p)]) {
                        LOGGER.debug(Long.toHexString(l));
                    }
                    RemoteJob remoteJob = new RemoteJob(chunk_ids[peers.indexOf(p)], graphFormat.getGraphFormatReader().getClass());
                    jobService.pushJobRemote(remoteJob, p);
                }
                boolean valid = true;
                long id = ChunkID.INVALID_ID;
                for (int j = chunkCreator.getApproxChunkAmount() / peers.size(); j >= 0; j--) {
                    if (chunk_ids[peers.size() - 1][j] == 0) continue;
                    id = chunk_ids[peers.size() - 1][j];
                }
                FileChunk fileChunk = new FileChunk(id);

                boolean finished = false;
                while(!finished){
                    sleep(100);
                    try {
                        finished = jobService.waitForRemoteJobsToFinish();
                    }catch(Exception e){
                        finished = false;
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
}
