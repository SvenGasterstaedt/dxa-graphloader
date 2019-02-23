package de.hhu.bsinfo.dxgraphloader.formats;

import de.hhu.bsinfo.dxgraphloader.formats.readers.EdgeListReader;
import de.hhu.bsinfo.dxgraphloader.formats.splitter.JSONGraphFileChunkCreator;
import de.hhu.bsinfo.dxgraphloader.loader.formats.GraphFormat;

public class JSONGraph extends GraphFormat {

    String file;


    public JSONGraph(final String... files) {
        super(files);

        CYCLES = 2;

        //suppors only single files
        file = files[0];
        //key format properties
        this.setFileChunkCreator(new JSONGraphFileChunkCreator(file, 16777216));
        this.setGraphFormatReader(new EdgeListReader());
    }
}
