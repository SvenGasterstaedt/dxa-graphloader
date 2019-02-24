package de.hhu.bsinfo.dxgraphloader.formats.readers;

import de.hhu.bsinfo.dxgraphloader.graph.data.Vertex;
import de.hhu.bsinfo.dxgraphloader.loader.data.DistributedObjectTable;
import de.hhu.bsinfo.dxgraphloader.loader.data.PeerVertexMap;
import de.hhu.bsinfo.dxgraphloader.loader.formats.GraphFormatReader;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.locks.Lock;

@SuppressWarnings("Duplicates")
public class NeighborListReader extends GraphFormatReader {

    public NeighborListReader(DistributedObjectTable distributedObjectTable) {
        super(distributedObjectTable);
    }

    @Override
    public boolean execute(byte[] content) {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(content)));
        try {
            while (bufferedReader.ready()) {
                String line = bufferedReader.readLine();
                if (line.charAt(0) == '#') continue;

                String[] key = line.split("\t");
                createVertex(key[0],Vertex.class, "");

                for (int i = 1; i < key.length; i++) {
                    createVertex(key[i],Vertex.class, "");
                }
            }
        } catch (
                IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}
