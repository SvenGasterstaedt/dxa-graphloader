package de.hhu.bsinfo.dxapp.job;

import de.hhu.bsinfo.dxapp.GraphLoaderApp;
import de.hhu.bsinfo.dxapp.data.FileChunk;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RemoteJob extends AbstractJob {


    public static final short Type_ID = 77;
    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());


    private String graphFormatReader;

    public RemoteJob() {
        super();
    }

    public RemoteJob(final String graphFormatReader) {
        this.graphFormatReader = graphFormatReader;
    }


    @Override
    public short getTypeID() {
        return Type_ID;
    }

    @Override
    public void execute() {
        LOGGER.debug("Job started,NE!");
        ChunkService chunkService = getService(ChunkService.class);
        FileChunk fileChunk = new FileChunk();
        while (chunkService.get().get(fileChunk)) {
            if (fileChunk.isIDValid() && fileChunk.isStateOk()) {
                LOGGER.debug(new String(fileChunk.getData()));
            }
        }
    }
}