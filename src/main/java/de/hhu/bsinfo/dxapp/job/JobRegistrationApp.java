package de.hhu.bsinfo.dxapp.job;

import de.hhu.bsinfo.dxapp.GraphLoaderApp;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.job.JobRuntimeException;
import de.hhu.bsinfo.dxram.job.JobService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JobRegistrationApp extends AbstractApplication {
    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());


    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return BuildConfig.DXRAM_VERSION;
    }

    @Override
    public String getApplicationName() {
        return "JobRegistrationApp";
    }


    @Override
    public void main(String[] p_args) {
        BootService bootService = getService(BootService.class);
        JobService jobService = getService(JobService.class);
        String[] jobs = p_args;

        //Register jobs
        for (String job_str : jobs)
            try {
                Class job = Class.forName(job_str);
                if (AbstractJob.class.isAssignableFrom(job)) {//could be extended for functions etc.
                    jobService.registerJobType(((Class<? extends AbstractJob>) job).newInstance().getTypeID(), job);
                    LOGGER.info("Job " + job.getSimpleName() + " registered!");
                } else {
                    LOGGER.warn(job_str + " is no job!");
                    continue;
                }
            } catch (JobRuntimeException jobEx) {
                LOGGER.info("Job already registered!");
            } catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
                LOGGER.error("Failed to register job!");
            }
    }

    @Override
    public void signalShutdown() {
    }
}


