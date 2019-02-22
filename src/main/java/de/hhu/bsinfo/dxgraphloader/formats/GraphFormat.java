package de.hhu.bsinfo.dxgraphloader.formats;

import de.hhu.bsinfo.dxgraphloader.formats.parsers.GraphFormatReader;
import de.hhu.bsinfo.dxgraphloader.formats.splitter.FileChunkCreator;

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


