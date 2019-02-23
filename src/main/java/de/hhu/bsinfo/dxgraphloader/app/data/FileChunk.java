package de.hhu.bsinfo.dxgraphloader.app.data;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class FileChunk extends AbstractChunk {


    private byte[] data;
    private boolean hasNext = true;


    public FileChunk(final long p_id) {
        this.setID(p_id);
    }

    public FileChunk(final byte[] fileData) {
        data = fileData;
        setID(ChunkID.INVALID_ID);
    }

    public byte[] getContents() {
        return data;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeByteArray(data);
        p_exporter.writeBoolean(hasNext);
    }


    @Override
    public void importObject(final Importer p_importer) {
        data = p_importer.readByteArray(data);
        hasNext = p_importer.readBoolean(hasNext);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofByteArray(data) + ObjectSizeUtil.sizeofBoolean();
    }
}