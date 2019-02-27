package de.hhu.bsinfo.dxgraphloader.loader.data;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.util.Arrays;

public class ChunkIDArray extends AbstractChunk {

    private long[] ids;

    public ChunkIDArray(final long chunkId) {
        super();
        setID(chunkId);
    }

    public ChunkIDArray(final long[] ids) {
        this.ids = ids;
    }

    public ChunkIDArray(final Long[] ids) {
        this.ids = new long[ids.length];
        for(int i = 0; i < ids.length; i++){
            this.ids[i] = ids[i];
        }
    }

    public long getChunkID(int index) {
        if(index > -1 && index < ids.length){
            return ids[index];
        }
        return ChunkID.INVALID_ID;
    }

    public long[] getIds(){
        return ids;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(ids.length);
        for (long t_id : ids) {
            p_exporter.writeLong(t_id);
        }
    }

    @Override
    public void importObject(final Importer p_importer) {
        int size = 0;
        size = p_importer.readInt(size);
        ids = new long[size];
        Arrays.fill(ids, ChunkID.INVALID_ID);
        for (int i = 0; i < size; i++) {
            ids[i] = p_importer.readLong(ids[i]);
        }
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + Long.BYTES * ids.length;
    }
}
