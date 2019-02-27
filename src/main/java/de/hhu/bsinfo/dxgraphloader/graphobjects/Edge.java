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

public class Edge extends AbstractChunk {

    protected long from   = ChunkID.INVALID_ID;
    protected long to     = ChunkID.INVALID_ID;

    public Edge(long id){
        setID(id);
    }

    public Edge(long p_id,long from,long to){
        super(p_id);
        this.from = from;
        this.to = to;
    }

    public long getFrom() {
        return from;
    }

    public void setEndPoint(Vertex start, Vertex end){
        from = start.getID();
        to = end.getID();

        start.addNeighbor(this.getID());
        end.addNeighbor(this.getID());
    }

    public long getTo() {
        return to;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public void setTo(long to) {
        this.to = to;
    }

    @Override
    public void exportObject(Exporter exporter) {
        exporter.writeLong(from);
        exporter.writeLong(to);
    }

    @Override
    public void importObject(Importer importer) {
        importer.readLong(from);
        importer.readLong(to);
    }

    @Override
    public int sizeofObject() {
        return 2*8;
    }
}
