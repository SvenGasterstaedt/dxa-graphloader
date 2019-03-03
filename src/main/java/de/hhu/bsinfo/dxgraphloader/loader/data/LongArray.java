/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxgraphloader.loader.data;

import java.util.Arrays;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public final class LongArray extends AbstractChunk {

    private long[] m_ids;

    public LongArray(final long p_chunkId) {
        super();
        setID(p_chunkId);
    }

    public LongArray() {
        super();
    }

    @SuppressWarnings("unused")
    public LongArray(final long[] p_ids) {
        m_ids = p_ids;
    }

    public LongArray(final Long[] p_ids) {
        m_ids = new long[p_ids.length];
        for (int i = 0; i < p_ids.length; i++) {
            m_ids[i] = p_ids[i];
        }
    }

    @SuppressWarnings("unused")
    public long getChunkID(int p_index) {
        if (p_index > -1 && p_index < m_ids.length) {
            return m_ids[p_index];
        }
        return ChunkID.INVALID_ID;
    }

    public long[] getIds() {
        return m_ids;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_ids.length);
        for (long id : m_ids) {
            p_exporter.writeLong(id);
        }
    }

    @SuppressWarnings("Duplicates")
    @Override
    public void importObject(final Importer p_importer) {
        int size = 0;
        size = p_importer.readInt(size);
        m_ids = new long[size];
        Arrays.fill(m_ids, ChunkID.INVALID_ID);
        for (int i = 0; i < size; i++) {
            m_ids[i] = p_importer.readLong(m_ids[i]);
        }
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + Long.BYTES * m_ids.length;
    }
}
