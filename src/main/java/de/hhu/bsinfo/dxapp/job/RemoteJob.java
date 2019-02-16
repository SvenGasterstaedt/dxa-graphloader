package de.hhu.bsinfo.dxapp.job;

import de.hhu.bsinfo.dxapp.GraphLoaderApp;
import de.hhu.bsinfo.dxapp.data.FileChunk;
import de.hhu.bsinfo.dxapp.formats.parsers.GraphFormatReader;
import de.hhu.bsinfo.dxapp.io.Util;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static java.lang.Thread.sleep;

public class RemoteJob extends AbstractJob {

    public static final short Type_ID = 78;
    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());


    private long c_id;
    private String classPath;

    public RemoteJob() {
        super();
    }

    public RemoteJob(final long p_ids, Class<? extends GraphFormatReader> graphFormatReader) {
        this.c_id = p_ids;
        this.classPath = graphFormatReader.getCanonicalName();
    }


    @Override
    public short getTypeID() {
        return Type_ID;
    }

    @Override
    public void execute() {
        LOGGER.debug("Job started!");
        ChunkService chunkService = getService(ChunkService.class);
        BootService bootService = getService(BootService.class);
        GraphFormatReader graphFormatReader = null;
        LOGGER.debug("Chunk ID: " + Long.toHexString(c_id));
        try {
            graphFormatReader = ((Class<? extends GraphFormatReader>) Class.forName(classPath)).newInstance();

            FileChunk fileChunk = new FileChunk(c_id);
            chunkService.get().get(fileChunk);
            LOGGER.debug("next Chunk ID: " + Long.toHexString(Util.fixChunkIDs(fileChunk.getNextID())));

            while(fileChunk.hasNext()){
                while(fileChunk.hasNext()){
                    chunkService.get().get(fileChunk);
                    LOGGER.debug("next Chunk ID: " + Long.toHexString(fileChunk.getNextID()));
                    if(fileChunk.getNextID()==ChunkID.INVALID_ID){
                        sleep(10);
                        chunkService.get().get(fileChunk);
                    }else{
                        c_id = fileChunk.getNextID();
                        break;
                    }
                }
                graphFormatReader.execute(fileChunk.getContents(), chunkService, bootService.getNodeID());
                LOGGER.debug(Long.toHexString(c_id) + " loaded!");
                if(!fileChunk.hasNext()){
                    break;
                }
                chunkService.remove().remove(fileChunk);
                LOGGER.debug("Chunk ID: " + Long.toHexString(c_id));
                fileChunk = new FileChunk(c_id);
                chunkService.get().get(fileChunk);
            }
        } catch (InstantiationException | ClassNotFoundException | IllegalAccessException | InterruptedException e) {
            e.printStackTrace();
        }
        LOGGER.debug("Finished!");

    }

    @Override
    public void importObject(final Importer p_importer) {
        super.importObject(p_importer);
        c_id = p_importer.readLong(c_id);
        c_id = Util.fixChunkIDs(c_id);
        classPath = p_importer.readString(classPath);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        super.exportObject(p_exporter);
        p_exporter.writeLong(c_id);
        p_exporter.writeString(classPath);
    }

    @Override
    public int sizeofObject() {
        return super.sizeofObject() + Long.BYTES + ObjectSizeUtil.sizeofString(classPath);
    }

}