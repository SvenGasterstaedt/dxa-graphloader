package de.hhu.bsinfo.dxgraphloader.app;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.app.data.ChunkIDArray;
import de.hhu.bsinfo.dxgraphloader.app.data.PeerVertexMap;
import de.hhu.bsinfo.dxgraphloader.graph.data.Vertex;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class LoadChunkManagerJob extends AbstractJob {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());


    private long chunkArrayID;
    private String classPath;
    private int workerCount;

    private PeerVertexMap peerVertexMap = new PeerVertexMap();

    public LoadChunkManagerJob() {
        super();
    }

    public LoadChunkManagerJob(long chunkID, String graphLoader, int workerCount) {
        this.chunkArrayID = chunkID;
        this.classPath = graphLoader;
        if (workerCount >= 1) {
            this.workerCount = workerCount;
        } else {
            this.workerCount = 1;
            LOGGER.warn("The amount of workers can't be smaller then 1!");
        }

    }

    @Override
    public void execute() {
        ChunkService chunkService = getService(ChunkService.class);
        ChunkLocalService chunkLocalService = getService(ChunkLocalService.class);
        BootService bootService = getService(BootService.class);
        JobService jobService = getService(JobService.class);
        NetworkService networkService = getService(NetworkService.class);

        ChunkIDArray chunkIDArray = new ChunkIDArray(chunkArrayID);
        chunkLocalService.getLocal().get(chunkIDArray);
        Queue<Long> chunkList = new ConcurrentLinkedQueue<>();
        List<LoadChunkLocalJob> jobList = new ArrayList<>();
        Lock lock = new ReentrantLock(true);


        long[] chunkIDs = chunkIDArray.getIds();

        for (long id : chunkIDs) {
            chunkList.add(id);
            LOGGER.debug(Long.toHexString(id) + " added to queue!");
        }
        chunkService.remove().remove(chunkIDArray);

        for (int i = 0; i < workerCount - 1; i++) {
            LoadChunkLocalJob abstractJob = new LoadChunkLocalJob(chunkList, classPath, lock, peerVertexMap);
            jobList.add(abstractJob);
            jobService.pushJob(abstractJob);
        }

        LoadChunkLocalJob localJob = new LoadChunkLocalJob(chunkList, classPath, lock, peerVertexMap);
        localJob.setServicesForLocal(chunkService, chunkLocalService, bootService);
        jobList.add(localJob);
        localJob.execute();

        Vertex[] vertices = peerVertexMap.values().stream().map(Vertex::new).toArray(Vertex[]::new);
        //resize duo to changing neighbor amount.
        chunkService.resize().resize(vertices);
        chunkService.put().put(vertices);
        LOGGER.debug("Finished Loading Chunks!");
    }


    @Override
    public void importObject(final Importer p_importer) {
        super.importObject(p_importer);
        chunkArrayID = p_importer.readLong(chunkArrayID);
        classPath = p_importer.readString(classPath);
        workerCount = p_importer.readInt(workerCount);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        super.exportObject(p_exporter);
        p_exporter.writeLong(chunkArrayID);
        p_exporter.writeString(classPath);
        p_exporter.writeInt(workerCount);
    }

    @Override
    public int sizeofObject() {
        return super.sizeofObject() + Long.BYTES + ObjectSizeUtil.sizeofString(classPath) + Integer.BYTES;
    }
}
