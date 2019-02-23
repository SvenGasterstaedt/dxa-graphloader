package de.hhu.bsinfo.dxgraphloader.loader.formats;

public abstract class GraphFormat {
    FileChunkCreator fileChunkCreator;
    GraphFormatReader formatReader;
    String[] files;

    public short CYCLES;

    public GraphFormat(final String... files) {
        this.files = files;
    }

    public FileChunkCreator getFileChunkCreator(short cycle) {
        fileChunkCreator.setCycle(cycle);
        return fileChunkCreator;
    }


    public GraphFormatReader getGraphFormatReader() {
        return formatReader;
    }

    public void setGraphFormatReader(GraphFormatReader formatReader){
        this.formatReader = formatReader;
    }

    public void setFileChunkCreator(FileChunkCreator fileChunkCreator){
        this.fileChunkCreator = fileChunkCreator;
    }
}


