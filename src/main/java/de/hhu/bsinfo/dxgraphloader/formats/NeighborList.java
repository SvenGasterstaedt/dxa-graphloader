package de.hhu.bsinfo.dxgraphloader.formats;

import de.hhu.bsinfo.dxgraphloader.loader.formats.GraphFormat;
import de.hhu.bsinfo.dxgraphloader.formats.readers.NeighborListReader;
import de.hhu.bsinfo.dxgraphloader.formats.splitter.SkippingLineNumberFileChunkCreator;

public class NeighborList extends GraphFormat {

    String file;

    public NeighborList(final String... files) {
        super(files);

        CYCLES = 1;

        this.setFileChunkCreator(new SkippingLineNumberFileChunkCreator(files[0], 16777216));
        this.setGraphFormatReader(new NeighborListReader());
    }
}

