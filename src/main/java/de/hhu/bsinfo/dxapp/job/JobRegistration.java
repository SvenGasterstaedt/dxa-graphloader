package de.hhu.bsinfo.dxapp.job;

import de.hhu.bsinfo.dxapp.GraphLoaderApp;
import de.hhu.bsinfo.dxram.app.ApplicationService;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;

public class JobRegistration {
    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());
    private BootService bootService;
    private ApplicationService applicationService;

    public JobRegistration(BootService bootService, ApplicationService applicationService) {
        this.bootService = bootService;
        this.applicationService = applicationService;
    }

    @SafeVarargs
    public final boolean registerJob(final Class<? extends AbstractJob>... jobs) {
        List<Short> peers = bootService.getOnlinePeerNodeIDs();
        String[] parameters = Arrays.stream(jobs).map(Class::getCanonicalName).toArray(String[]::new);
        for (short p : peers) {
            try {
                if (!applicationService.startApplication(p, JobRegistrationApp.class.getCanonicalName(), parameters)) {
                    LOGGER.error("Starting " + JobRegistrationApp.class.getCanonicalName() + " failed!");
                    return false;
                } else {
                    while (applicationService.getApplicationsRunning().contains(JobRegistrationApp.class.getCanonicalName())) {
                        wait(10);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return true;
    }
}