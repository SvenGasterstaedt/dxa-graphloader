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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.graphobjects.Edge;
import de.hhu.bsinfo.dxgraphloader.graphobjects.Vertex;
import de.hhu.bsinfo.dxgraphloader.loader.KeyCreator;
import de.hhu.bsinfo.dxgraphloader.loader.data.Graph;
import de.hhu.bsinfo.dxgraphloader.util.Tuple;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;

public abstract class AbstractGraphFormatReader {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());

    private Graph m_graph;

    private ConcurrentHashMap<Tuple<Long, Long>, Long> m_edges;
    private ConcurrentHashMap<Long, Long> m_vertexMap;
    private ArrayList<Set<Long>> m_remoteKeys;
    private ChunkLocalService m_chunkLocal;
    private BootService m_boot;

    protected AbstractGraphFormatReader(Graph p_graph,
            ConcurrentHashMap<Long, Long> p_vertexMap,
            ConcurrentHashMap<Tuple<Long, Long>, Long> p_edges,
            ArrayList<Set<Long>> p_remoteKeys,
            ChunkLocalService p_chunkLocal,
            BootService p_boot) {
        m_graph = p_graph;
        m_vertexMap = p_vertexMap;
        m_edges = p_edges;
        m_remoteKeys = p_remoteKeys;
        m_chunkLocal = p_chunkLocal;
        m_boot = p_boot;
    }

    public abstract void readVertices(final byte[] p_content);

    public abstract void readEdges(final byte[] p_content);

    protected void createVertex(String p_key, Object... p_args) {
        try {
            KeyCreator keyCreator = m_graph.getKeyCreator();
            long key = keyCreator.createNumKey(p_key);
            Vertex vertex = new Vertex();
            if (KeyCreator.getPeer(key) == m_boot.getNodeID()) {
                Long value = m_vertexMap.putIfAbsent(keyCreator.createNumKey(p_key), ChunkID.INVALID_ID);
                if (value == null) {
                    m_chunkLocal.createLocal().create(vertex);
                    m_vertexMap.replace(keyCreator.createNumKey(p_key), vertex.getID());
                }
            } else {
                //LOGGER.info(keyCreator.getPeerIndex(KeyCreator.getPeer(key)));
                m_remoteKeys.get(keyCreator.peerIndex(key)).add(key);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void createEdge(String p_vrtx1, String p_vtrx2, Object... p_args) {
        try {
            KeyCreator keyCreator = m_graph.getKeyCreator();
            Tuple<Long, Long> t = new Tuple<>(keyCreator.createNumKey(p_vrtx1), keyCreator.createNumKey(p_vtrx2));
            Edge edge = new Edge();
            m_chunkLocal.createLocal().create(edge);
            m_edges.putIfAbsent(t, edge.getID());
        } catch (
                Exception e) {
            e.printStackTrace();
        }

    }
}
