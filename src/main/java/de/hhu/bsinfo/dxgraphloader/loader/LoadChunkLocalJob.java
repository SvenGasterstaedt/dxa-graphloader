package de.hhu.bsinfo.dxgraphloader.loader;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.loader.data.DistributedObjectTable;
import de.hhu.bsinfo.dxgraphloader.loader.data.FileChunk;
import de.hhu.bsinfo.dxgraphloader.loader.data.PeerVertexMap;
import de.hhu.bsinfo.dxgraphloader.loader.formats.GraphFormatReader;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Lock;

import static java.lang.Thread.sleep;

public class LoadChunkLocalJob extends AbstractJob {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());
    private static final Class[] graphFormatConstructor = new Class[]{DistributedObjectTable.class};

    private Queue<Long> queue;
    private String classPath;
    private DistributedObjectTable distributedObjectTable;

    private ChunkService chunkService;
    private ChunkLocalService chunkLocalService;
    private BootService bootService;
    private List<Set<String>> vertices = new ArrayList<>();
    private Lock queueLock;

    private boolean finished = false;

    public LoadChunkLocalJob(final Queue<Long> queue, final String classPath, final Lock queueLock, final DistributedObjectTable distributedObjectTable) {
        this.queue = queue;
        this.classPath = classPath;
        this.queueLock = queueLock;
        this.distributedObjectTable = distributedObjectTable;
        for (int i = 0; i < distributedObjectTable.getPeerSize(); i++) {
            vertices.add(new ConcurrentSkipListSet<>());
        }
    }

    public void setServicesForLocal(final BootService bootService,final ChunkService chunkService, final ChunkLocalService chunkLocalService) {
        this.bootService = bootService;
        this.chunkService = chunkService;
        this.chunkLocalService = chunkLocalService;
    }

    @Override
    public void execute() {
        if (chunkService == null || bootService == null) {
            chunkLocalService = getService(ChunkLocalService.class);
            bootService = getService(BootService.class);
            chunkService = getService(ChunkService.class);
        }
        if (chunkService == null || bootService == null || chunkLocalService == null) {
            LOGGER.error("Started Job on local scope without setting Services");
            return;
        }

        while (true) {
            queueLock.lock();
            if (queue.size() > 0) {
                long chunkID = queue.remove();
                queueLock.unlock();
                FileChunk fileChunk = new FileChunk(chunkID);
                chunkLocalService.getLocal().get(fileChunk);

                try {

                    GraphFormatReader graphFormatReader = (GraphFormatReader) Class.forName(classPath)
                            .getConstructor(graphFormatConstructor)
                            .newInstance(distributedObjectTable);

                    graphFormatReader.execute(fileChunk.getContents());

                    List<Set<String>> vertices = graphFormatReader.getVertices();

                    for(int i = 0; i < vertices.size();i++){
                        this.vertices.get(i).addAll(vertices.get(i));
                    }

                    LOGGER.debug(Long.toHexString(chunkID) + " loaded!");

                } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException | InstantiationException | IllegalAccessException e) {
                    e.printStackTrace();
                }

                chunkService.remove().remove(fileChunk);

            } else {
                queueLock.unlock();
                break;
            }

            try {
                sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        finished = true;
        LOGGER.debug("Finished Load Chunk!");

    }

    public boolean isFinished() {
        return finished;
    }

    public List<Set<String>> getVertices() {
        return vertices;
    }
}
