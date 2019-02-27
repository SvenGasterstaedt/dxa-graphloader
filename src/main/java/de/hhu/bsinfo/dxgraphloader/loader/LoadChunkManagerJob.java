package de.hhu.bsinfo.dxgraphloader.loader;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.graph.data.Vertex;
import de.hhu.bsinfo.dxgraphloader.loader.data.ChunkIDArray;
import de.hhu.bsinfo.dxgraphloader.loader.data.GraphObject;
import de.hhu.bsinfo.dxgraphloader.loader.data.StringArray;
import de.hhu.bsinfo.dxgraphloader.loader.data.StringArrayFinder;
import de.hhu.bsinfo.dxgraphloader.util.IDUtils;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierStatus;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static de.hhu.bsinfo.dxgraphloader.loader.GraphLoader.LOAD_LOCK;
import static java.lang.Thread.sleep;


public class LoadChunkManagerJob extends AbstractJob {


    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());

    private long chunkArrayID;
    private String classPath;
    private int workerCount;

    private long distributedObjectTableID;

    private ConcurrentHashMap<String, Long> localVertexMap = new ConcurrentHashMap<String, Long>();
    private Queue<HashSet<String>> remoteQueue = new ConcurrentLinkedQueue<>();

    public LoadChunkManagerJob() {
        super();
    }

    public LoadChunkManagerJob(long distributedObjectTableID, long chunkID, String graphLoader, int workerCount) {

        this.chunkArrayID = chunkID;
        this.classPath = graphLoader;

        this.distributedObjectTableID = distributedObjectTableID;
        this.workerCount = workerCount;

    }

    @Override
    public void execute() {
        //get service
        LOGGER.info("Started %s from %s", LoadChunkManagerJob.class.getSimpleName(), GraphLoaderApp.class.getSimpleName());
        ChunkService chunkService = getService(ChunkService.class);
        ChunkLocalService chunkLocalService = getService(ChunkLocalService.class);
        BootService bootService = getService(BootService.class);
        JobService jobService = getService(JobService.class);
        NameserviceService nameserviceService = getService(NameserviceService.class);
        SynchronizationService synchronizationService = getService(SynchronizationService.class);

        GraphObject graphObject = new GraphObject(distributedObjectTableID);
        chunkService.get().get(graphObject);

        ChunkIDArray chunkIDArray = new ChunkIDArray(chunkArrayID);
        chunkLocalService.getLocal().get(chunkIDArray);
        Queue<Long> chunkList = new ConcurrentLinkedQueue<>();
        List<LoadChunkLocalJob> jobList = new ArrayList<>();

        Lock queueLock = new ReentrantLock(true);

        int cycleBarrier = BarrierID.INVALID_ID;
        while (cycleBarrier == BarrierID.INVALID_ID) {
            cycleBarrier = (int) nameserviceService.getChunkID(GraphLoader.CYCLE_LOCK, 1000);
        }


        long[] chunkIDs = chunkIDArray.getIds();

        for (long id : chunkIDs) {
            chunkList.add(id);
            //LOGGER.debug(Long.toHexString(id) + " added to queue!");
        }
        chunkService.remove().remove(chunkIDArray);

        {
            int loadBarrier = BarrierID.INVALID_ID;
            while (loadBarrier == BarrierID.INVALID_ID) {
                loadBarrier = (int) nameserviceService.getChunkID(LOAD_LOCK, 1000);
            }

            LOGGER.info("Extracting and Creating Local Vertices!");
            ArrayList<Set<String>> remoteKeySet = new ArrayList<>();
            for (short p : bootService.getOnlinePeerNodeIDs()) {
                remoteKeySet.add(Collections.newSetFromMap((new ConcurrentHashMap<String, Boolean>())));
            }


            for (int i = 0; i < workerCount - 1; i++) {
                LoadChunkLocalJob abstractJob = new LoadChunkLocalJob(chunkList, classPath, queueLock, graphObject, localVertexMap, remoteKeySet);
                jobList.add(abstractJob);
                jobService.pushJob(abstractJob);
            }

            LoadChunkLocalJob localJob = new LoadChunkLocalJob(chunkList, classPath, queueLock, graphObject, localVertexMap, remoteKeySet);
            localJob.setServicesForLocal(bootService, chunkService, chunkLocalService, nameserviceService, synchronizationService);
            jobList.add(localJob);
            localJob.execute();

            while (!areAllJobsFinished(jobList)) {
                try {
                    sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            jobList.clear();

            LOGGER.info("Storing Remote Keys!");
            StringArrayFinder finder = new StringArrayFinder(bootService.getOnlinePeerNodeIDs().size());

            long[] keys_id = new long[bootService.getOnlinePeerNodeIDs().size()];

            for (int i = 0; i < bootService.getOnlinePeerNodeIDs().size(); i++) {

                if (bootService.getOnlinePeerNodeIDs().get(i) == bootService.getNodeID()) continue;

                Set<String> set = remoteKeySet.get(i);

                long[] ids = new long[set.size() / 5324];
                Arrays.fill(ids, ChunkID.INVALID_ID);

                LOGGER.info("Keys for Peer: '%s'", IDUtils.shortToHexString(bootService.getOnlinePeerNodeIDs().get(i)));
                int pos = 0;
                HashSet<String> smallSet = new HashSet<>();
                Iterator<String> iterator = set.iterator();
                while (iterator.hasNext()) {
                    smallSet.add(iterator.next());

                    if (smallSet.size() > 5324 * 32 || !iterator.hasNext()) {
                        final StringArray stringArray = new StringArray(smallSet);
                        while (stringArray.getID() == ChunkID.INVALID_ID) {
                            chunkService.create().create(bootService.getOnlinePeerNodeIDs().get(i), stringArray);
                        }
                        boolean status = false;
                        do {
                            status = chunkService.put().put(stringArray);
                        } while (!status);
                        LOGGER.debug("Created %s!", Long.toHexString(stringArray.getID()));
                        ids[pos] = stringArray.getID();
                        pos++;
                        smallSet = new HashSet<>();
                    }
                }
                finder.add(i, ids);
            }
            while (finder.getID() == ChunkID.INVALID_ID) {
                chunkService.create().create(bootService.getNodeID(), finder);
                chunkService.put().put(finder);
            }

            LOGGER.info("Waiting for other peers ...");
            BarrierStatus status = synchronizationService.barrierSignOn(loadBarrier, finder.getID(), true);

            LOGGER.info("Starting Synchronization!");
            long[] finderIDs = status.getCustomData();
            StringArrayFinder[] finders = new StringArrayFinder[finderIDs.length];
            for (int i = 0; i < finderIDs.length; i++) {
                finders[i] = new StringArrayFinder(finderIDs[i]);
                chunkService.get().get(finders);
                for (long l : finders[i].getArray(graphObject.getPeerPos(bootService.getNodeID()))) {
                    if (l == ChunkID.INVALID_ID) continue;
                    LOGGER.debug("Stored '%s'", Long.toHexString(l));
                    StringArray str = new StringArray(l);
                    chunkService.get().get(str);
                    for (String s : str.getKeys()) {
                        Long value = localVertexMap.putIfAbsent(s, ChunkID.INVALID_ID);
                        if (value == null) {
                            Vertex v = new Vertex();
                            chunkLocalService.createLocal().create(v);
                            localVertexMap.replace(s, v.getID());
                        }
                    }
                    chunkService.remove().remove(str);
                }
            }

            for (long l : localVertexMap.values()) {
                chunkService.put().put(new Vertex());
            }

            LOGGER.info("'%s' holds '%s' Vertices", IDUtils.shortToHexString(bootService.getNodeID()), localVertexMap.values().size());
        }
        synchronizationService.barrierSignOn(cycleBarrier, ChunkID.INVALID_ID, true);
    }

    private static boolean areAllJobsFinished(List<LoadChunkLocalJob> job) {
        for (LoadChunkLocalJob j : job) {
            if (!j.isFinished()) {
                return false;
            }
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