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

import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public final class PeerVertexMap extends AbstractChunk {

    private final HashMap<Long, Long> m_map = new HashMap<>();

    public PeerVertexMap() {
    }

    public PeerVertexMap(long p_id) {
        setID(p_id);
    }

    public PeerVertexMap(final HashMap<Long, Long> p_map) {
        m_map.putAll(p_map);
    }

    public HashMap<Long, Long> getMap() {
        return m_map;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(m_map.size());
        for (Map.Entry<Long, Long> entry : m_map.entrySet()) {
            p_exporter.writeLong(entry.getKey());
            p_exporter.writeLong(entry.getValue());
        }
    }

    @Override
    public void importObject(Importer p_importer) {
        int size = 0;
        size = p_importer.readInt(size);
        if (size > 0) {
            for (int i = 0; i < size; i++) {
                long key = 0;
                long id = ChunkID.INVALID_ID;
                key = p_importer.readLong(key);
                id = p_importer.readLong(id);
                m_map.put(key, id);
            }
        }
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + Long.BYTES * m_map.size() * 2;
    }
}
