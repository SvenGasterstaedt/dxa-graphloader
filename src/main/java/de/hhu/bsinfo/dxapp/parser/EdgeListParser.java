package de.hhu.bsinfo.dxapp.parser;

import de.hhu.bsinfo.dxapp.data.Edge;
import de.hhu.bsinfo.dxapp.data.LabeledVertex;
import de.hhu.bsinfo.dxapp.data.Vertex;

import java.io.IOException;

public class EdgeListParser extends SimpleFormatParser {

    private final String delimiter;
    EdgeListParser(byte[] chunk, String delimiter) {
        super(chunk);
        this.delimiter = delimiter;
    }

    @Override
    public Edge[] getEdges() {
        return new Edge[0];
    }

    //lines has the format <vertex1>[delimiter]<vertex2>
    @Override
    public Vertex[] getVertices() {
        try {
                String[] vertices = readLine().split(delimiter);
                return new LabeledVertex[]{new LabeledVertex(vertices[0]), new LabeledVertex(vertices[1])};
        } catch (Exception e) {
            return null;
        }
    }
}
