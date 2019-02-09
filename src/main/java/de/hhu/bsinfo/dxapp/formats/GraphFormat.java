package de.hhu.bsinfo.dxapp.formats;

import de.hhu.bsinfo.dxapp.formats.parsers.GraphFormatReader;
import de.hhu.bsinfo.dxapp.formats.split.FileChunkCreator;

public abstract class GraphFormat {
    FileChunkCreator fileChunkCreator;
    GraphFormatReader formatReader;
    String[] files;

    GraphFormat(final String... files) {
        this.files = files;
    }

    public FileChunkCreator getFileChunkCreator() {
        return fileChunkCreator;
    }


    public GraphFormatReader getGraphFormatReader() {
        return formatReader;
    }
}


