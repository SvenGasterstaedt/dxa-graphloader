package de.hhu.bsinfo.dxgraphloader.formats;

import de.hhu.bsinfo.dxgraphloader.formats.readers.EdgeListReader;
import de.hhu.bsinfo.dxgraphloader.formats.splitter.SkippingLineFileChunkCreator;
import de.hhu.bsinfo.dxgraphloader.loader.formats.GraphFormat;

public class EdgeList extends GraphFormat {

    public EdgeList(final String... files) {
        super(files);

        CYCLES = 1;

        //suppors only single files
        this.setFileChunkCreator(new SkippingLineFileChunkCreator(files[0], 16777216));
        this.setGraphFormatReader(new EdgeListReader());

    }
}

