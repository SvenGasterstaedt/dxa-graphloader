package de.hhu.bsinfo.dxapp.formats;

import de.hhu.bsinfo.dxapp.formats.parsers.EdgeListReader;
import de.hhu.bsinfo.dxapp.formats.split.SkippingFileChunkCreator;

public class EdgeListFormat extends GraphFormat {

    String file;

    EdgeListFormat(final String... files) {
        super(files);
        file = files[0];
        try {
            fileChunkCreator = new SkippingFileChunkCreator(file, 4096);
            formatReader = new EdgeListReader();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
