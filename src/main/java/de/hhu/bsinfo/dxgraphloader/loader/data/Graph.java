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
import java.util.List;
import java.util.Map;

import de.hhu.bsinfo.dxgraphloader.loader.KeyCreator;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public final class Graph extends AbstractChunk {

    private HashMap<Short, Long> m_listPeerMaps = new HashMap<>();
    private KeyCreator m_keyCreator = new KeyCreator();

    public Graph(){
    }

    public Graph(long p_id) {
        setID(p_id);
    }

    public Graph(final List<Short> p_peers, final ChunkService p_chunk) {
        m_keyCreator = new KeyCreator(p_peers);
        for (short p : p_peers) {
            PeerVertexMap peerVertexMap = new PeerVertexMap();
            p_chunk.create().create(p, peerVertexMap);
            p_chunk.put().put(peerVertexMap);
            m_listPeerMaps.put(p, peerVertexMap.getID());
        }
    }

    public long getMap(short p_node) {
        return m_listPeerMaps.get(p_node);
    }

    public KeyCreator getKeyCreator() {
        return m_keyCreator;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(m_listPeerMaps.size());
        for (Map.Entry<Short, Long> entry : m_listPeerMaps.entrySet()) {
            p_exporter.writeShort(entry.getKey());
            p_exporter.writeLong(entry.getValue());
        }
        p_exporter.exportObject(m_keyCreator);
    }

    @Override
    public void importObject(Importer p_importer) {
        int size = 0;
        size = p_importer.readInt(size);
        short shrt = 0;
        long l = 0;
        for (int i = 0; i < size; i++) {
            shrt = p_importer.readShort(shrt);
            l = p_importer.readLong(l);
            m_listPeerMaps.put(shrt, l);
        }
        p_importer.importObject(m_keyCreator);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + Short.BYTES * m_listPeerMaps.size() + Long.BYTES * m_listPeerMaps.size() +
                m_keyCreator.sizeofObject();
    }
}