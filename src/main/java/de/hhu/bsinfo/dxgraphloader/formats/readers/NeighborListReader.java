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

import de.hhu.bsinfo.dxgraphloader.graph.data.Vertex;
import de.hhu.bsinfo.dxgraphloader.loader.data.GraphObject;
import de.hhu.bsinfo.dxgraphloader.loader.formats.GraphFormatReader;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <h1>NeighborListReader</h1>
 * Reader for EdgeList:
 * [vertex1][tab][vertex2][tab][vertex3][tab][vertex4][tab][vertex5][\n]
 *
 * line wise.
 *
 * @author Sven Gasterstaedt
 * @version 1.0
 * @since 2019-03-15
 */

public final class NeighborListReader extends GraphFormatReader {


    public NeighborListReader(GraphObject graphObject, ConcurrentHashMap<String, Long> peerVertexMap, ArrayList<Set<String>> remoteKeys, ChunkLocalService chunkLocalService, BootService bootService) {
        super(graphObject, peerVertexMap, remoteKeys, chunkLocalService, bootService);
    }

    @Override
    public void readVertices(byte[] content) {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(content)));
        try {
            char[] lineChar = new char[4];
            bufferedReader.read(lineChar);
            long lineNumber = Long.getLong(new String(lineChar));
            while (bufferedReader.ready()) {
                String line = bufferedReader.readLine();
                if (line.charAt(0) == '#') continue;
                String[] key = line.split("\t");
                String key1 = Long.toString(lineNumber);
                createVertex(key1, Vertex.class);
                for (int i = 0; i < key.length; i++) {
                    createVertex(key[i], Vertex.class);
                }
                lineNumber++;
            }
            bufferedReader.close();
        } catch (
                IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void readEdges(byte[] content) {
    }
}
