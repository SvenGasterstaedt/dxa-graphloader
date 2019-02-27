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

package de.hhu.bsinfo.dxgraphloader.loader.formats;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.graphobjects.Edge;
import de.hhu.bsinfo.dxgraphloader.graphobjects.Vertex;
import de.hhu.bsinfo.dxgraphloader.loader.data.GraphObject;
import de.hhu.bsinfo.dxgraphloader.util.Tuple;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;

public abstract class AbstractGraphFormatReader {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());

    private GraphObject m_graph;

    private Set<Tuple<String, String>> m_edges = new HashSet<>();
    private ConcurrentHashMap<String, Long> m_vertexMap;
    private ArrayList<Set<String>> m_remoteKeys;
    private ChunkLocalService m_chunkLocal;
    private BootService m_boot;
    private Class<? extends Vertex> m_vertexclass;

    @SuppressWarnings("ConstructorNotProtectedInAbstractClass")
    public AbstractGraphFormatReader(GraphObject p_graph, ConcurrentHashMap<String, Long> p_vertexMap,
            ArrayList<Set<String>> p_remoteKeys, ChunkLocalService p_chunkLocal, BootService p_boot) {
        m_graph = p_graph;
        m_vertexMap = p_vertexMap;
        m_remoteKeys = p_remoteKeys;
        m_chunkLocal = p_chunkLocal;
        m_boot = p_boot;
    }

    public abstract void readVertices(final byte[] p_content);

    public abstract void readEdges(final byte[] p_content);

    protected void createVertex(String p_id, Object... p_args) {
        Vertex vertex = new Vertex();
        short targetPeer = m_graph.getNode(p_id.hashCode() % m_graph.getPeerSize());
        if (targetPeer == m_boot.getNodeID()) {
            Long value = m_vertexMap.putIfAbsent(p_id, ChunkID.INVALID_ID);
            if (value == null) {
                m_chunkLocal.createLocal().create(vertex);
                m_vertexMap.replace(p_id, vertex.getID());
            }
        } else {
            m_remoteKeys.get(m_graph.getPeerPos(targetPeer)).add(p_id);
        }
    }

    protected void createEdge(String p_from, String p_to, Class<? extends Edge> p_edge, Object... p_args) {
        m_edges.add(new Tuple<>(p_from, p_to));
    }

    public Set<Tuple<String, String>> getEdges() {
        return m_edges;
    }

}
