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

package de.hhu.bsinfo.dxgraphloader.graphobjects;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class Edge extends AbstractChunk {

    long[] m_connect = new long[] {ChunkID.INVALID_ID, ChunkID.INVALID_ID};

    public Edge() {
    }

    public Edge(long id) {
        setID(id);
    }

    public Edge(long p_id, long p_vrtxID1, long p_vrtxID2) {
        super(p_id);
        m_connect[0] = p_vrtxID1;
        m_connect[1] = p_vrtxID2;
    }

    public void setEndPoint(Vertex p_vrtx1, Vertex p_vrtx2) {
        m_connect[0] = p_vrtx1.getID();
        m_connect[1] = p_vrtx2.getID();
        p_vrtx1.addNeighbor(getID());
        p_vrtx2.addNeighbor(getID());
    }

    public long[] getVertices() {
        return m_connect;
    }

    public void setConnection(int p_index, long p_vrtxID) {
        if (p_index > 0 && p_index < 2) {
            m_connect[p_index] = p_vrtxID;
        }
    }

    @Override
    public void exportObject(Exporter exporter) {
        exporter.writeLongArray(m_connect);
    }

    @Override
    public void importObject(Importer importer) {
        m_connect = importer.readLongArray(m_connect);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofLongArray(m_connect);
    }
}
