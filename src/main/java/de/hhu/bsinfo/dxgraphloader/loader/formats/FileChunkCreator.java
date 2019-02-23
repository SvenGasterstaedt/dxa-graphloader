package de.hhu.bsinfo.dxgraphloader.loader.formats;

import de.hhu.bsinfo.dxgraphloader.loader.data.FileChunk;

public abstract class FileChunkCreator {

    abstract public boolean hasRemaining();
    abstract public FileChunk getNextChunk();
    abstract public int getApproxChunkAmount();

    abstract public void setCycle(short cycle);
}
