package de.hhu.bsinfo.dxapp.formats;

import de.hhu.bsinfo.dxapp.formats.split.FileChunkCreator;

public abstract class GraphFormat {
    public FileChunkCreator fileChunkCreator;
    public GraphFormat graphFormat;

    GraphFormat(final String[] files) {
    }

    public FileChunkCreator getFileChunkCreator(){
        return fileChunkCreator;
    }
}

