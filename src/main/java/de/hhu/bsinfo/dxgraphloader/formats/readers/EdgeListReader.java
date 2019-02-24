package de.hhu.bsinfo.dxgraphloader.formats.readers;

import de.hhu.bsinfo.dxgraphloader.graph.data.Vertex;
import de.hhu.bsinfo.dxgraphloader.loader.data.DistributedObjectTable;
import de.hhu.bsinfo.dxgraphloader.loader.formats.GraphFormatReader;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class EdgeListReader extends GraphFormatReader {
    public EdgeListReader(DistributedObjectTable distributedObjectTable) {
        super(distributedObjectTable);
    }

    @Override
    public boolean execute(byte[] content) {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(content)));
        try {
            while (bufferedReader.ready()) {
                String line = bufferedReader.readLine();
                if (line.charAt(0) == '#') continue;
                int index = line.indexOf('\t');
                String key1 = line.substring(0, index);
                int index2 = line.substring(index + 1).indexOf('\t');
                String key2;
                if(index2!=-1) {
                    key2 = line.substring(index+1, index+1+index2);
                }else{
                    key2 = line.substring(index+1);
                }

                //u could use here any own Vertex implementation as long as its extends Vertex!
                createVertex(key1, Vertex.class,"");
                createVertex(key2, Vertex.class,"");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}
