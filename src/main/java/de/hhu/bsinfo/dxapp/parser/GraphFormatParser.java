package de.hhu.bsinfo.dxapp.parser;

import de.hhu.bsinfo.dxapp.data.Edge;
import de.hhu.bsinfo.dxapp.data.Vertex;

import java.io.IOException;

public abstract class GraphFormatParser {
    protected byte[] chunk;

    GraphFormatParser(byte[] chunk){

    }

    abstract public boolean ready() throws IOException;

    abstract public Edge[] getEdges();

    abstract public Vertex[] getVertices();
}
