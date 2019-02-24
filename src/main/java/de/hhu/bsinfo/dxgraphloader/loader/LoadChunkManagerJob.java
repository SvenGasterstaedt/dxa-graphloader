package de.hhu.bsinfo.dxgraphloader.loader;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.graph.data.Vertex;
import de.hhu.bsinfo.dxgraphloader.loader.data.ChunkIDArray;
import de.hhu.bsinfo.dxgraphloader.loader.data.DistributedObjectTable;
import de.hhu.bsinfo.dxgraphloader.loader.data.PeerVertexMap;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Thread.sleep;


public class LoadChunkManagerJob extends AbstractJob {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());

    private long chunkArrayID;
    private String classPath;
    private int workerCount;

    private long distributedObjectTableID;

    public LoadChunkManagerJob() {
        super();
    }

    public LoadChunkManagerJob(long distributedObjectTableID, long chunkID, String graphLoader, int workerCount) {

        this.chunkArrayID = chunkID;
        this.classPath = graphLoader;

        this.distributedObjectTableID = distributedObjectTableID;

        if (workerCount >= 1) {
            this.workerCount = workerCount;
        } else {
            this.workerCount = 1;
            LOGGER.warn("The amount of workers can't be smaller then 1!");
        }

    }

    @Override
    public void execute() {
        LOGGER.info("Started %s from %s", LoadChunkManagerJob.class.getSimpleName(), GraphLoaderApp.class.getSimpleName());
        ChunkService chunkService = getService(ChunkService.class);
        ChunkLocalService chunkLocalService = getService(ChunkLocalService.class);
        BootService bootService = getService(BootService.class);
        JobService jobService = getService(JobService.class);

        DistributedObjectTable distributedObjectTable = new DistributedObjectTable(distributedObjectTableID);
        chunkService.get().get(distributedObjectTable);
        PeerVertexMap peerVertexMap = new PeerVertexMap(distributedObjectTable.getMap(bootService.getNodeID()));
        chunkService.get().get(peerVertexMap);

        ChunkIDArray chunkIDArray = new ChunkIDArray(chunkArrayID);
        chunkLocalService.getLocal().get(chunkIDArray);
        Queue<Long> chunkList = new ConcurrentLinkedQueue<>();
        List<LoadChunkLocalJob> jobList = new ArrayList<>();
        Lock queueLock = new ReentrantLock(true);

        long[] chunkIDs = chunkIDArray.getIds();

        for (long id : chunkIDs) {
            chunkList.add(id);
            //LOGGER.debug(Long.toHexString(id) + " added to queue!");
        }
        chunkService.remove().remove(chunkIDArray);

        for (int i = 0; i < workerCount - 1; i++) {
            LoadChunkLocalJob abstractJob = new LoadChunkLocalJob(chunkList, classPath, queueLock, distributedObjectTable);
            jobList.add(abstractJob);
            jobService.pushJob(abstractJob);
        }

        LoadChunkLocalJob localJob = new LoadChunkLocalJob(chunkList, classPath, queueLock, distributedObjectTable);
        localJob.setServicesForLocal(bootService, chunkService, chunkLocalService);
        jobList.add(localJob);
        localJob.execute();

        //wait for all jobs to finish!
        while (!areAllJobsFinished(jobList)) {
            try {
                sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        LOGGER.info("Extracting and Creating Local Vertices!");
        //extract all loaded vertices and store them. HUGE PAYLOAD!!!! Streams??
        List<Set<String>> vertices = new ArrayList<>();
        for (int i = 0; i < distributedObjectTable.getPeerSize(); i++) {
            vertices.add(new ConcurrentSkipListSet<>());
        }
        for (int i = 0; i < distributedObjectTable.getPeerSize(); i++) {
            for (LoadChunkLocalJob job : jobList) {
                vertices.get(i).addAll(job.getVertices().get(i));
            }
            if (distributedObjectTable.getNode(i) == bootService.getNodeID()) {
                vertices.get(i).stream().forEach(key -> {
                    Vertex v = new Vertex();
                    chunkLocalService.createLocal().create(v);
                    peerVertexMap.put(key, v.getID());
                });
            }
        }
        LOGGER.info("Syncronizing!");
        //SyncChunkJob syncChunkJob = new SyncChunkJob();
        LOGGER.debug("Finished Loading Chunks!");
    }


    private static boolean areAllJobsFinished(List<LoadChunkLocalJob> job) {
        for (LoadChunkLocalJob j : job) {
            if (!j.isFinished()) return false;
        }
        return true;
    }

    @Override
    public void importObject(final Importer p_importer) {
        super.importObject(p_importer);
        chunkArrayID = p_importer.readLong(chunkArrayID);
        classPath = p_importer.readString(classPath);
        workerCount = p_importer.readInt(workerCount);
        distributedObjectTableID = p_importer.readLong(distributedObjectTableID);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        super.exportObject(p_exporter);
        p_exporter.writeLong(chunkArrayID);
        p_exporter.writeString(classPath);
        p_exporter.writeInt(workerCount);
        p_exporter.writeLong(distributedObjectTableID);
    }

    @Override
    public int sizeofObject() {
        return super.sizeofObject() + Long.BYTES + ObjectSizeUtil.sizeofString(classPath) + Integer.BYTES + Long.BYTES;
    }
}
