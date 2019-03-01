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

import java.util.Arrays;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class Vertex extends AbstractChunk {

    private long[] m_edges;

    @SuppressWarnings("WeakerAccess")
    public Vertex(long p_id) {
        m_edges = new long[1];
        setID(p_id);
    }

    public Vertex() {
        m_edges = new long[1];
    }

    @SuppressWarnings("WeakerAccess")
    public Vertex(long p_id, int p_neighborCount) {
        super(p_id);
        m_edges = new long[p_neighborCount];
    }

    void addNeighbor(long p_edgeID) {
        m_edges = Arrays.copyOf(m_edges, m_edges.length + 1);
        m_edges[m_edges.length - 1] = p_edgeID;
    }

    void addNeighbors(long[] p_edgeIDs) {
        m_edges = Arrays.copyOf(m_edges, m_edges.length + 1);
        System.arraycopy(p_edgeIDs, 0, m_edges, m_edges.length, p_edgeIDs.length);
    }

    public void mergeVertex(Vertex p_vertex, ChunkService p_chunk) {
        for (long l : p_vertex.m_edges) {
            Edge edge = new Edge(l);
            p_chunk.get().get(edge);
            if (edge.from == p_vertex.getID()) {
                edge.setFrom(getID());
            } else {
                edge.setTo(getID());
            }
            p_chunk.put().put(edge);
        }
        addNeighbors(p_vertex.m_edges);
        p_chunk.remove().remove(p_vertex);
        p_chunk.resize().resize(this);
        p_chunk.put().put(this);
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(m_edges.length);
        for (long l : m_edges) {
            p_exporter.writeLong(l);
        }
    }

    @SuppressWarnings("Duplicates")
    @Override
    public void importObject(Importer p_importer) {
        int size = 0;
        size = p_importer.readInt(size);
        m_edges = new long[size];
        for (int i = 0; i < size; i++) {
            m_edges[i] = p_importer.readLong(m_edges[i]);
        }
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + Long.BYTES * m_edges.length;
    }
}
