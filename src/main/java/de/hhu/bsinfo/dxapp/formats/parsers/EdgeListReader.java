package de.hhu.bsinfo.dxapp.formats.parsers;

import de.hhu.bsinfo.dxapp.data.Edge;
import de.hhu.bsinfo.dxapp.data.LabeledVertex;
import de.hhu.bsinfo.dxapp.data.Vertex;

public class EdgeListReader extends SimpleFormatReader {

    private final String delimiter;

    EdgeListReader(byte[] chunk, String delimiter) {
        super(chunk);
        this.delimiter = delimiter;
    }

    public Edge[] getEdges() {
        return new Edge[0];
    }

    //lines has the format <vertex1>[delimiter]<vertex2>

    public Vertex[] getVertices() {
        try {
            String[] vertices = readLine().split(delimiter);
            return new LabeledVertex[]{new LabeledVertex(vertices[0]), new LabeledVertex(vertices[1])};
        } catch (Exception e) {
            return null;
        }
    }
}
