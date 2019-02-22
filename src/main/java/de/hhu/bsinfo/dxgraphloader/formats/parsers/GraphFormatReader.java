package de.hhu.bsinfo.dxgraphloader.formats.parsers;

import de.hhu.bsinfo.dxram.chunk.ChunkService;

public abstract class GraphFormatReader {

    public GraphFormatReader() {
    }

    public abstract boolean execute(final byte[] content,final ChunkService chunkService,final short current_peer);
}
