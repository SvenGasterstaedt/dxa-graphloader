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

import java.util.Arrays;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class Vertex extends AbstractChunk {

    private int m_size = 1;
    private int m_index = 0;

    private long[] m_edges = new long[1];

    public void incSize(){
        m_size++;
    }

    public int getSize(){
        return m_size;
    }

    public void resizeArray(int p_size){
        m_edges = Arrays.copyOf(m_edges, m_edges.length+p_size);
    }

    public Vertex(long p_id) {
        setID(p_id);
    }

    public Vertex() {
        m_edges = new long[1];
    }


    public void addNeighbor(long p_edgeID) {
        if(m_index < m_edges.length){
            m_edges[m_index] = p_edgeID;
            m_index++;
        }else{
            m_edges = Arrays.copyOf(m_edges, m_edges.length+1);
            m_edges[m_index] = p_edgeID;
            m_index++;
        }
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeLongArray(m_edges);
        p_exporter.writeInt(m_size);
        p_exporter.writeInt(m_index);
    }

    @SuppressWarnings("Duplicates")
    @Override
    public void importObject(Importer p_importer) {
        m_edges = p_importer.readLongArray(m_edges);
        m_size = p_importer.readInt(m_size);
        m_index = p_importer.readInt(m_index);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES * 2 + ObjectSizeUtil.sizeofLongArray(m_edges);
    }
}
