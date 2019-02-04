package de.hhu.bsinfo.dxapp.formats;

import de.hhu.bsinfo.dxapp.formats.split.FileChunkCreator;
import de.hhu.bsinfo.dxapp.formats.split.SkippingFileChunkCreator;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

public class EdgeListFormat extends GraphFormat{

    String file;

    EdgeListFormat(final String[] files) throws Exception {
        super(files);
        file = files[0];
        fileChunkCreator = new SkippingFileChunkCreator(file,4096);
    }
}
