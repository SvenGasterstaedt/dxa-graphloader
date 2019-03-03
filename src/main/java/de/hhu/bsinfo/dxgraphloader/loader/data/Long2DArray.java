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

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public final class Long2DArray extends AbstractChunk {

    private long[][] m_chunks = new long[0][0];

    @SuppressWarnings("unused")
    public Long2DArray() {
    }

    public Long2DArray(long p_id) {
        setID(p_id);
    }

    public Long2DArray(int p_size) {
        m_chunks = new long[p_size][];
        for (int i = 0; i < m_chunks.length; i++) {
            m_chunks[i] = new long[0];
        }
    }

    public void add(int p_index, long[] p_id) {
        if (p_index >= 0 && p_index < m_chunks.length) {
            m_chunks[p_index] = p_id;
        }
    }

    public long[] getArray(int p_pos) {
        return m_chunks[p_pos];
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(m_chunks.length);
        for (long[] longArray : m_chunks) {
            p_exporter.writeLongArray(longArray);
        }
    }

    @Override
    public void importObject(Importer p_importer) {
        int size = 0;
        size = p_importer.readInt(size);
        m_chunks = new long[size][];
        for (int i = 0; i < size; i++) {
            m_chunks[i] = p_importer.readLongArray(m_chunks[i]);
        }
    }

    @Override
    public int sizeofObject() {
        int size = Integer.BYTES;
        for (long[] longArray : m_chunks) {
            size += ObjectSizeUtil.sizeofLongArray(longArray);
        }
        return size;
    }
}
