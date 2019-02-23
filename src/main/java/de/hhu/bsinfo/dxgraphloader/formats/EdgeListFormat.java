package de.hhu.bsinfo.dxgraphloader.formats;

import de.hhu.bsinfo.dxgraphloader.formats.parsers.EdgeListReader;
import de.hhu.bsinfo.dxgraphloader.formats.splitter.SkippingLineSplitter;

public class EdgeListFormat extends GraphFormat {

    String file;

    EdgeListFormat(final String... files) {
        super(files);
        file = files[0];
        try {
            fileChunkCreator = new SkippingLineSplitter(file, 16777216);
            formatReader = new EdgeListReader();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
