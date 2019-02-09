package de.hhu.bsinfo.dxapp.job;

import de.hhu.bsinfo.dxapp.GraphLoaderApp;
import de.hhu.bsinfo.dxapp.data.FileChunk;
import de.hhu.bsinfo.dxapp.formats.parsers.GraphFormatReader;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RemoteJob extends AbstractJob {

    public static final short Type_ID = 78;
    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());


    private long[] p_ids;
    private Class<? extends GraphFormatReader> graphFormatReader;

    public RemoteJob() {
        super();
    }

    public RemoteJob(final long[] p_ids, Class<? extends GraphFormatReader> graphFormatReader) {
        this.p_ids = p_ids;
        this.graphFormatReader = graphFormatReader;
    }


    @Override
    public short getTypeID() {
        return Type_ID;
    }

    @Override
    public void execute() {
        LOGGER.debug("Job started!");
        ChunkService chunkService = getService(ChunkService.class);
        FileChunk fileChunk;
        for (int i = 0; i < p_ids.length; i++) {
            if (p_ids[i] != 0) {
                fileChunk = new FileChunk(p_ids[i]);

                LOGGER.debug(Long.toHexString(p_ids[i]));
                chunkService.get().get(fileChunk);
                if (fileChunk.isIDValid() && fileChunk.isStateOk())
                    LOGGER.debug(new String(fileChunk.getContents()));
            } else {
                continue;
            }
        }
        LOGGER.debug(graphFormatReader.getCanonicalName());
        LOGGER.info("Job finished!");

    }

    @Override
    public void importObject(final Importer p_importer) {
        super.importObject(p_importer);
        p_ids = p_importer.readLongArray(p_ids);

        //removes values on the left then on the right and inserts them on their position
        for (int i = 0; i < p_ids.length; i++) {
            p_ids[i] = p_ids[i] << 56
                    ^ p_ids[i] << 48 >>> 56 << 48
                    ^ p_ids[i] << 40 >>> 56 << 40
                    ^ p_ids[i] << 32 >>> 56 << 32
                    ^ p_ids[i] << 24 >>> 56 << 24
                    ^ p_ids[i] << 16 >>> 56 << 16
                    ^ p_ids[i] << 8 >>> 56 << 8
                    ^ p_ids[i] >>> 56;
        }
        try {
            String name = "";
            name = p_importer.readString(name);
            Class clazz = Class.forName(name);
            if (GraphFormatReader.class.isAssignableFrom(clazz)) {
                graphFormatReader = (Class<? extends GraphFormatReader>) clazz;
            } else {
                throw new ClassNotFoundException();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        super.exportObject(p_exporter);
        p_exporter.writeLongArray(p_ids);
        p_exporter.writeString(graphFormatReader.getCanonicalName());
    }

    @Override
    public int sizeofObject() {
        return super.sizeofObject() + ObjectSizeUtil.sizeofLongArray(p_ids) + ObjectSizeUtil.sizeofString(graphFormatReader.getCanonicalName());
    }
}