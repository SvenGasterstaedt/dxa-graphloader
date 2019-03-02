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

import de.hhu.bsinfo.dxgraphloader.loader.data.Graph;
import de.hhu.bsinfo.dxgraphloader.loader.formats.AbstractGraphFormatReader;
import de.hhu.bsinfo.dxgraphloader.util.Tuple;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;

/**
 * <h1>EdgeListReader</h1>
 * Reader for EdgeList:
 * [vertex1][tab][vertex2][\n]
 * line wise.
 *
 * @author Sven Gasterstaedt
 * @version 1.0
 * @since 2019-03-15
 */

public final class EdgeListReader extends AbstractGraphFormatReader {

    public EdgeListReader(Graph p_graph,
            ConcurrentHashMap<Long, Long> p_vertexMap,
            ConcurrentHashMap<Tuple<Long, Long>, Long> p_edges,
            ArrayList<Set<Long>> p_remoteKeys, ChunkLocalService p_chunkLocal, BootService p_boot) {
        super(p_graph, p_vertexMap, p_edges, p_remoteKeys, p_chunkLocal, p_boot);
    }

    @Override
    public void readVertices(byte[] p_content) {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(p_content)));
        try {
            while (bufferedReader.ready()) {
                String line = bufferedReader.readLine();
                if (line.charAt(0) == '#') {
                    continue;
                }
                int index = line.indexOf('\t');
                createVertex(line.substring(0, index));
                int index2 = line.substring(index + 1).indexOf('\t');
                if (index2 != -1) {
                    createVertex(line.substring(index + 1, index + 1 + index2));
                } else {
                    createVertex(line.substring(index + 1));
                }
            }
            bufferedReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void readEdges(byte[] p_content) {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(p_content)));
        try {
            while (bufferedReader.ready()) {
                String line = bufferedReader.readLine();
                if (line.charAt(0) == '#') {
                    continue;
                }
                int index = line.indexOf('\t');
                int index2 = line.substring(index + 1).indexOf('\t');
                if (index2 != -1) {
                    createEdge(line.substring(0, index), line.substring(index + 1, index + 1 + index2));
                } else {
                    createEdge(line.substring(0, index), line.substring(index + 1));
                }
            }
            bufferedReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
