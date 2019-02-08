package de.hhu.bsinfo.dxapp.data;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class FileChunk extends AbstractChunk {

    byte[] data;

    public FileChunk(final byte[] data) {
        this.data = data;
    }

    public FileChunk(final long p_id, final byte[] data) {
        super(p_id);
        this.data = data;
    }

    public FileChunk() {

    }

    public byte[] getData() {
        return data;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeByteArray(data);
    }

    @Override
    public void importObject(Importer p_importer) {
        p_importer.readByteArray(data);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofByteArray(data);
    }
}
