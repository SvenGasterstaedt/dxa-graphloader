package de.hhu.bsinfo.dxapp.formats.split;

import de.hhu.bsinfo.dxapp.data.FileChunk;

public abstract class FileChunkCreator {

    abstract public boolean hasRemaining();
    abstract public FileChunk getNextChunk();
}
