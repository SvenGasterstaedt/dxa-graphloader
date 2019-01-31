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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.loader.KeyCreator;
import de.hhu.bsinfo.dxgraphloader.loader.data.Graph;
import de.hhu.bsinfo.dxgraphloader.util.Tuple;

public abstract class AbstractGraphFormatReader {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());

    private Graph m_graph;

    private Set<Tuple> m_edges;
    private ArrayList<Set<Long>> m_vertices;

    protected AbstractGraphFormatReader(Graph p_graph,
            ArrayList<Set<Long>> p_vertices,
            Set<Tuple> p_edges) {
        m_graph = p_graph;
        m_edges = p_edges;
        m_vertices = p_vertices;
    }

    public abstract void readVertices(final byte[] p_content);

    public abstract void readEdges(final byte[] p_content);

    protected void storeVertexKey(String p_key, Object... p_args) {
        try {
            KeyCreator keyCreator = m_graph.getKeyCreator();
            long key = keyCreator.createNumKey(p_key);
            m_vertices.get(keyCreator.peerIndex(key)).add(key);
        } catch (
                Exception e) {
            e.printStackTrace();
        }

    }

    protected void createEdge(String p_vrtx1, String p_vtrx2, Object... p_args) {
        try {
            KeyCreator keyCreator = m_graph.getKeyCreator();
            Tuple t = new Tuple(keyCreator.createNumKey(p_vrtx1), keyCreator.createNumKey(p_vtrx2));
            m_edges.add(t);
        } catch (
                Exception e) {
            e.printStackTrace();
        }
    }
}
