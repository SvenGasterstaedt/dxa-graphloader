package de.hhu.bsinfo.dxapp.formats.split;

public abstract class FileChunkCreator {

    abstract public boolean hasRemaining();
    abstract public byte[] getNextChunk();
}
