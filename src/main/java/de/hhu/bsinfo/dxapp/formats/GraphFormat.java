package de.hhu.bsinfo.dxapp.parser;

import de.hhu.bsinfo.dxapp.data.Edge;
import de.hhu.bsinfo.dxapp.data.Vertex;
import de.hhu.bsinfo.dxapp.split.FileChunkCreator;

import java.io.IOException;

public abstract class GraphFormat {
    FileChunkCreator fileChunkCreator;
    GraphFormat graphFormat;

    abstract GraphFormat(String[] files);
}

