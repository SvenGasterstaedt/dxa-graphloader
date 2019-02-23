package de.hhu.bsinfo.dxgraphloader.formats;

import de.hhu.bsinfo.dxgraphloader.app.data.formats.GraphFormat;
import de.hhu.bsinfo.dxgraphloader.formats.readers.EdgeListReader;
import de.hhu.bsinfo.dxgraphloader.formats.splitter.SkippingLineSplitter;

public class EdgeList extends GraphFormat {

    String file;

    public EdgeList(final String... files) {
        super(files);

        CYCLES = 1;

        //suppors only single files
        file = files[0];
        try {
            //key format properties
            setFileChunkCreator(new SkippingLineSplitter(file, 16777216));

            setGraphFormatReader(new EdgeListReader());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

