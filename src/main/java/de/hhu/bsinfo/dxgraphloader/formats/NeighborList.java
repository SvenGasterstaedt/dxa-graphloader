package de.hhu.bsinfo.dxgraphloader.formats;

import de.hhu.bsinfo.dxgraphloader.app.data.formats.GraphFormat;
import de.hhu.bsinfo.dxgraphloader.formats.readers.NeighborListReader;
import de.hhu.bsinfo.dxgraphloader.formats.splitter.NeighborLineSplitter;

public class NeighborList extends GraphFormat {

    String file;

    public NeighborList(final String... files) {
        super(files);

        CYCLES = 1;

        file = files[0];
        try {
            setFileChunkCreator(new NeighborLineSplitter(file, 16777216));
            setGraphFormatReader(new NeighborListReader());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

