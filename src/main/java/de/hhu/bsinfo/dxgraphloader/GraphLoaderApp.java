package de.hhu.bsinfo.dxgraphloader;

import de.hhu.bsinfo.dxgraphloader.loader.GraphLoader;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import de.hhu.bsinfo.dxram.job.JobService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * "GraphLoaderApp"
 **/

@SuppressWarnings("Duplicates")
public class GraphLoaderApp extends AbstractApplication {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());

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
        final BootService bootService = getService(BootService.class);
        final JobService jobService = getService(JobService.class);
        final ChunkService chunkService = getService(ChunkService.class);
        final GraphLoader graphLoader = new GraphLoader(bootService, jobService, chunkService);

        if (p_args.length < 3) {
            LOGGER.error("Parameters required! <int numWorkers> <boolean parseWhileReading> <String format> <Stringpath files>\nTerminated!");
            return;
        }
        int numWorkers;
        try {
            numWorkers = Integer.parseInt(p_args[0]);
        }catch(Exception e){
            LOGGER.error("Couldn't parse args!");
            return;
        }
        String format = p_args[1].toUpperCase();
        String[] file_paths = Arrays.copyOfRange(p_args, 2, p_args.length);
        if (!graphLoader.supportedFormats.isSupported(format)) {
            LOGGER.error(format + " is no not supported!");
            LOGGER.info("List of supported formats:");
            for (String f : graphLoader.supportedFormats.supportedFormats()) {
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
        //prints id of the distrubuted object table
        System.out.println(graphLoader.loadFormat(format,file_paths,numWorkers).getID());
    }

    @Override
    public void signalShutdown() {
    }
}

