package de.hhu.bsinfo.dxapp;

import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import de.hhu.bsinfo.dxram.job.JobService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;

/**
 * "GraphLoaderApp"
 **/
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
        BootService bootService = getService(BootService.class);
        List<Short> peers = bootService.getOnlinePeerNodeIDs();

        if (p_args.length < 2) {
            LOGGER.error("Parameters required! <format> <files>\nTerminated!");
            return;
        }

        String format = p_args[0];
        String[] file_paths = Arrays.copyOfRange(p_args, 1, p_args.length);

        JobService jobService = getService(JobService.class);
        GraphLoaderJob graphLoaderJob = null;
        try {
            graphLoaderJob = new GraphLoaderJob(format, file_paths, peers);

            long jobId = jobService.pushJob(graphLoaderJob);
            jobService.waitForLocalJobsToFinish();
            graphLoaderJob.didSucceed();
        } catch (Exception e) {
            LOGGER.error("GraphLoader terminated!");
        }
        return;
    }


    @Override
    public void signalShutdown() {
    }
}
