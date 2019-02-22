package de.hhu.bsinfo.dxgraphloader.formats.splitter;

import de.hhu.bsinfo.dxgraphloader.app.data.FileChunk;

public abstract class FileChunkCreator {

    abstract public boolean hasRemaining();
    abstract public FileChunk getNextChunk();
    abstract public int getApproxChunkAmount();
}
