package de.hhu.bsinfo.dxgraphloader.loader.data;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class LongPairArray extends AbstractChunk {

    private long[] m_keys;
    private long[] m_ids;

    public LongPairArray() {
    }

    public LongPairArray(long p_id) {
        setID(p_id);
    }

    public LongPairArray(int p_size) {
        m_keys = new long[p_size];
        m_ids = new long[p_size];
    }

    public LongPairArray(long[] p_keys, long[] p_ids) {
        m_keys = p_keys;
        m_ids = p_ids;
    }

    public int getSize() {
        return m_keys.length;
    }

    public long[] getKeys() {
        return m_keys;
    }

    public long[] getIDs() {
        return m_ids;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeLongArray(m_keys);
        p_exporter.writeLongArray(m_ids);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_keys = p_importer.readLongArray(m_keys);
        m_ids = p_importer.readLongArray(m_ids);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofLongArray(m_keys) + ObjectSizeUtil.sizeofLongArray(m_ids);
    }
}
