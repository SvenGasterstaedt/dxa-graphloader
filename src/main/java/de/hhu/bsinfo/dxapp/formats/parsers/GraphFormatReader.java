package de.hhu.bsinfo.dxapp.formats.parsers;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Importable;

public abstract class GraphFormatReader {

    public GraphFormatReader() {
    }

    public abstract boolean execute(byte[] content, ChunkService chunkService, short current_peer);
}
