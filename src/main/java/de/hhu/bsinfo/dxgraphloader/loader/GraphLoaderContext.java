package de.hhu.bsinfo.dxgraphloader.loader;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;

public final class GraphLoaderContext {

    private final BootService m_bootService;
    private final ChunkService m_chunkService;
    private final JobService m_jobService;
    private final NameserviceService m_nameserviceService;
    private final SynchronizationService m_synchronizationService;
    private final ChunkLocalService m_chunkLocalService;

    /**
     * This method is used to set up the GraphLoader
     * and adds all needed services
     *
     * @param p_bootService
     *         DxRam BootService from the current peer
     * @param p_jobService
     *         DxRam JobService from the current peer
     * @param p_chunkService
     *         DxRam ChunkService from the current peer
     * @param p_nameserviceService
     *         DxRam NameserviceService from the current peer
     * @param p_synchronizationService
     *         DxRam SynchronizationService from the current peer
     * @param p_chunkLocalService
     *         DxRam ChunkLocalService from the current peer
     */
    public GraphLoaderContext(BootService p_bootService, ChunkService p_chunkService,
            ChunkLocalService p_chunkLocalService, JobService p_jobService, NameserviceService p_nameserviceService,
            SynchronizationService p_synchronizationService) {
        m_bootService = p_bootService;
        m_chunkService = p_chunkService;
        m_jobService = p_jobService;
        m_nameserviceService = p_nameserviceService;
        m_synchronizationService = p_synchronizationService;
        m_chunkLocalService = p_chunkLocalService;
    }

    /**
     * @return BootService of the current peer
     */
    BootService getBootService() {
        return m_bootService;
    }

    /**
     * @return ChunkService of the current peer
     */
    ChunkService getChunkService() {
        return m_chunkService;
    }

    /**
     * @return JobService of the current peer
     */
    JobService getJobService() {
        return m_jobService;
    }

    /**
     * @return NameserviceService of the current peer
     */
    NameserviceService getNameserviceService() {
        return m_nameserviceService;
    }

    /**
     * @return SynchronizationService of the current peer
     */
    SynchronizationService getSynchronizationService() {
        return m_synchronizationService;
    }

    /**
     * @return ChunkLocalService of the current peer
     */
    ChunkLocalService getChunkLocalService() {
        return m_chunkLocalService;
    }
}
