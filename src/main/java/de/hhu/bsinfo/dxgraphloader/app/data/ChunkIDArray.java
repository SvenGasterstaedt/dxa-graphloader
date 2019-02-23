package de.hhu.bsinfo.dxgraphloader.app.data;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.Arrays;

public class ChunkIDArray extends AbstractChunk {

    private long[] ids;
    private boolean hasNext;
    int pos = 0;

    public ChunkIDArray(long chunkId) {
        super();
        setID(chunkId);
        hasNext = true;
    }

    public ChunkIDArray(int size) {
        super();
        ids = new long[size];
        Arrays.fill(ids, ChunkID.INVALID_ID);
        hasNext = true;
    }

    public void addID(long id) {
        ids[pos] = id;
        pos++;
    }

    public long getChunkID(int pos) {
        return ids[pos];
    }

    public boolean hasNext() {
        return hasNext;
    }

    public void setHasNext(boolean value) {
        hasNext = value;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeBoolean(hasNext);
        p_exporter.writeInt(ids.length);
        for (long t_id : ids) {
            p_exporter.writeLong(t_id);
        }
    }

    @Override
    public void importObject(Importer p_importer) {
        hasNext = p_importer.readBoolean(hasNext);
        int size = 0;
        size = p_importer.readInt(size);
        ids = new long[size];
        Arrays.fill(ids,ChunkID.INVALID_ID);
        for (int i = 0; i < size; i++) {
            ids[i] = p_importer.readLong(ids[i]);
        }
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + Long.BYTES * ids.length + ObjectSizeUtil.sizeofBoolean();
    }
}
