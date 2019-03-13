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

package de.hhu.bsinfo.dxgraphloader.loader.graphobjects;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class Edge extends AbstractChunk {

    private long connection1 = ChunkID.INVALID_ID;
    private long connection2 = ChunkID.INVALID_ID;

    public Edge() {
    }

    public Edge(long id) {
        setID(id);
    }

    public Edge(long p_id, long p_vrtxID1, long p_vrtxID2) {
        super(p_id);
        connection1 = p_vrtxID1;
        connection2 = p_vrtxID2;
    }

    public void setEndPoint(Vertex p_vrtx1, Vertex p_vrtx2) {
        connection1 = p_vrtx1.getID();
        connection2 = p_vrtx2.getID();
        p_vrtx1.addNeighbor(getID());
        p_vrtx2.addNeighbor(getID());
    }

    public void addConnection(long p_vrtxID) {
        if (connection1 == ChunkID.INVALID_ID) {
            connection1 = p_vrtxID;
        } else {
            connection2 = p_vrtxID;
        }
    }

    @Override
    public void exportObject(Exporter exporter) {
        exporter.writeLong(connection1);
        exporter.writeLong(connection2);
    }

    @Override
    public void importObject(Importer importer) {
        connection1 = importer.readLong(connection1);
        connection2 = importer.readLong(connection2);
    }

    @Override
    public int sizeofObject() {
        return Long.BYTES * 2;
    }
}
