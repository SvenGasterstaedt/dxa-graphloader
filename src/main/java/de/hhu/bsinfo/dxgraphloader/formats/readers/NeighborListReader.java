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

package de.hhu.bsinfo.dxgraphloader.formats.readers;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import de.hhu.bsinfo.dxgraphloader.loader.graphobjects.Vertex;
import de.hhu.bsinfo.dxgraphloader.loader.data.Graph;
import de.hhu.bsinfo.dxgraphloader.loader.formats.AbstractGraphFormatReader;
import de.hhu.bsinfo.dxgraphloader.util.Tuple;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;

/**
 * <h1>NeighborListReader</h1>
 * Reader for EdgeList:
 * [vertex1][tab][vertex2][tab][vertex3][tab][vertex4][tab][vertex5][\n]
 * line wise.
 *
 * @author Sven Gasterstaedt
 * @version 1.0
 * @since 2019-03-15
 */

public final class NeighborListReader extends AbstractGraphFormatReader {

    public NeighborListReader(Graph p_graph, ArrayList<Set<Long>> p_vertices,
            Set<Tuple> p_edges) {
        super(p_graph, p_vertices, p_edges);
    }

    @Override
    public void readVertices(byte[] p_content) {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(p_content)));
        try {
            char[] lineChar = new char[4];
            //noinspection ResultOfMethodCallIgnored
            bufferedReader.read(lineChar);
            long lineNumber = Long.getLong(new String(lineChar));
            while (bufferedReader.ready()) {
                String line = bufferedReader.readLine();
                if (line.charAt(0) == '#') {
                    continue;
                }
                String[] key = line.split("\t");
                String key1 = Long.toString(lineNumber);
                storeVertexKey(key1, Vertex.class);
                for (int i = 0; i < key.length; i++) {
                    storeVertexKey(key[i], Vertex.class);
                }
                lineNumber++;
            }
            bufferedReader.close();
        } catch (
                Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void readEdges(byte[] p_content) {
    }
}
