package de.hhu.bsinfo.dxapp.formats.parsers;

import de.hhu.bsinfo.dxapp.data.Edge;
import de.hhu.bsinfo.dxapp.data.Vertex;

public class EdgeListReader extends SimpleFormatReader {

    public EdgeListReader(byte[] chunk) {
        super(chunk);
    }

    public EdgeListReader() {
        super();
    }

    public Edge[] getEdges() {
        return new Edge[0];
    }

    //lines has the format <vertex1>[delimiter]<vertex2>

    public Vertex[] getVertices() {
        try {
            String[] vertices = readLine().split("\n");
            for (String s : vertices) {
                System.out.println(s);
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }
}
