package de.hhu.bsinfo.dxapp.job;

import de.hhu.bsinfo.dxapp.GraphLoaderApp;
import de.hhu.bsinfo.dxapp.data.FileChunk;
import de.hhu.bsinfo.dxapp.formats.parsers.GraphFormatReader;
import de.hhu.bsinfo.dxapp.io.Util;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RemoteJob extends AbstractJob {

    public static final short Type_ID = 78;
    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());


    private long[] p_ids;
    private String classPath;

    public RemoteJob() {
        super();
    }

    public RemoteJob(final long[] p_ids, Class<? extends GraphFormatReader> graphFormatReader) {
        this.p_ids = p_ids;
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
        NetworkService networkService = getService(NetworkService.class);
        BootService bootService = getService(BootService.class);
        try {
            GraphFormatReader graphFormatReader = ((Class<? extends GraphFormatReader>) Class.forName(classPath)).newInstance();
            FileChunk fileChunk;
            for (int i = 0; i < p_ids.length; i++) {
                if (p_ids[i] != 0) {
                    fileChunk = new FileChunk(p_ids[i]);
                    chunkService.get().get(fileChunk);
                    if (fileChunk.isIDValid() && fileChunk.isStateOk()) {
                        graphFormatReader.execute(fileChunk.getContents(), chunkService, bootService.getNodeID());
                        LOGGER.debug(Long.toHexString(p_ids[i]) + " loaded!");
                        chunkService.remove().remove(fileChunk);
                    }
                } else {
                    continue;
                }
            }

        } catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
            e.printStackTrace();
        }
        LOGGER.info("Job finished!");
    }

    @Override
    public void importObject(final Importer p_importer) {
        super.importObject(p_importer);
        p_ids = p_importer.readLongArray(p_ids);

        //removes values on the left then on the right and inserts them on their position
        for (int i = 0; i < p_ids.length; i++) {
            p_ids[i] = Util.fixChunkIDs(p_ids[i]);
        }
        classPath = p_importer.readString(classPath);

    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        super.exportObject(p_exporter);
        p_exporter.writeLongArray(p_ids);
        p_exporter.writeString(classPath);
    }

    @Override
    public int sizeofObject() {
        return super.sizeofObject() + ObjectSizeUtil.sizeofLongArray(p_ids) + ObjectSizeUtil.sizeofString(classPath);
    }

}