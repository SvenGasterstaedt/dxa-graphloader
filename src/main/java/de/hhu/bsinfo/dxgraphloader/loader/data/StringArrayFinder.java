package de.hhu.bsinfo.dxgraphloader.loader.data;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class StringArrayFinder extends AbstractChunk {

    long[][] chunks = new long[0][0];


    public StringArrayFinder() {
    }

    public StringArrayFinder(long c_id) {
        setID(c_id);
    }

    public StringArrayFinder(int size) {
        chunks = new long[size][];
        for(int i = 0; i < chunks.length; i++){
            chunks[i] = new long[0];
        }
    }

    public boolean add(int i, long[] id) {
        if (i >= 0 && i < chunks.length) {
            chunks[i] = id;
            return true;
        }
        return false;
    }

    public long[] getArray(int peerPos) {
        if (peerPos >= 0 && peerPos < chunks.length) {
            return chunks[peerPos];
        }
        return null;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(chunks.length);
        for (long[] l : chunks)
            p_exporter.writeLongArray(l);
    }

    @Override
    public void importObject(Importer p_importer) {
        int size = 0;
        size = p_importer.readInt(size);
        chunks = new long[size][];
        for (int i = 0; i < size; i++) {
            chunks[i] = p_importer.readLongArray(chunks[i]);
        }
    }

    @Override
    public int sizeofObject() {
        int size = Integer.BYTES;
        for (long[] l : chunks) {
            size += ObjectSizeUtil.sizeofLongArray(l);
        }
        return size;
    }
}
