package de.hhu.bsinfo.dxgraphloader.loader.formats;

public abstract class GraphFormat {
    FileChunkCreator fileChunkCreator;
    Class<? extends GraphFormatReader> formatReader;
    String[] files;

    public short CYCLES;

    public GraphFormat(final String... files) {
        this.files = files;
    }

    public FileChunkCreator getFileChunkCreator(short cycle) {
        fileChunkCreator.setCycle(cycle);
        return fileChunkCreator;
    }


    public Class<? extends GraphFormatReader> getGraphFormatReader() {
        return formatReader;
    }

    public void setGraphFormatReader(Class<? extends GraphFormatReader> formatReader){
        this.formatReader = formatReader;
    }

    public void setFileChunkCreator(FileChunkCreator fileChunkCreator){
        this.fileChunkCreator = fileChunkCreator;
    }
}


