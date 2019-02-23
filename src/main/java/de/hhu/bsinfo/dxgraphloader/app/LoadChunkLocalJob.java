package de.hhu.bsinfo.dxgraphloader.app;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.app.data.FileChunk;
import de.hhu.bsinfo.dxgraphloader.app.data.PeerVertexMap;
import de.hhu.bsinfo.dxgraphloader.app.data.formats.GraphFormatReader;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.Queue;
import java.util.concurrent.locks.Lock;

import static java.lang.Thread.sleep;

public class LoadChunkLocalJob extends AbstractJob {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());


    private Queue<Long> queue;
    private String classPath;

    private ChunkService chunkService;
    private ChunkLocalService chunkLocalService;
    private BootService bootService;
    private PeerVertexMap peerVertexMap;

    private Lock lock;

    public LoadChunkLocalJob(final Queue<Long> queue, final String classPath, final Lock lock, final PeerVertexMap peerVertexMap) {
        this.queue = queue;
        this.classPath = classPath;
        this.lock = lock;
        this.peerVertexMap = peerVertexMap;
    }

    public void setServicesForLocal(final ChunkService chunkService, final ChunkLocalService chunkLocalService, final BootService bootService) {
        this.chunkService = chunkService;
        this.chunkLocalService = chunkLocalService;
        this.bootService = bootService;
    }

    @Override
    public void execute() {
        if (chunkService == null || bootService == null || chunkLocalService == null) {
            chunkLocalService = getService(ChunkLocalService.class);
            bootService = getService(BootService.class);
            chunkService = getService(ChunkService.class);
        }
        if (chunkService == null || bootService == null || chunkLocalService == null) {
            LOGGER.error("Started Job on local scope without setting Services");
            return;
        }

        while (true) {
            lock.lock();
            if (queue.size() > 0) {
                long chunkID = queue.remove();
                lock.unlock();

                FileChunk fileChunk = new FileChunk(chunkID);
                chunkLocalService.getLocal().get(fileChunk);

                try {

                    GraphFormatReader graphFormatReader = (GraphFormatReader) Class.forName(classPath).getConstructor().newInstance();
                    graphFormatReader.execute(fileChunk.getContents(), chunkService, bootService.getNodeID(), peerVertexMap);
                    LOGGER.debug(Long.toHexString(chunkID) + " loaded!");

                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            } else {
                lock.unlock();
                break;
            }

            try {
                sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        LOGGER.debug("Finished Load Chunk!");

    }
}
