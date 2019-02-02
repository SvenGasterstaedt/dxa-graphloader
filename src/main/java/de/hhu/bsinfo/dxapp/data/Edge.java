package de.hhu.bsinfo.dxapp.data;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class Edge extends AbstractChunk {

    protected long from   = ChunkID.INVALID_ID;
    protected long to     = ChunkID.INVALID_ID;

    public Edge(){}

    public Edge(long p_id,long from,long to){
        super(p_id);
        this.from = from;
        this.to = to;
    }

    public long getFrom() {
        return from;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public long getTo() {
        return to;
    }

    public void setTo(long to) {
        this.to = to;
    }

    @Override
    public void exportObject(Exporter exporter) {
        exporter.writeLong(from);
        exporter.writeLong(to);
    }

    @Override
    public void importObject(Importer importer) {
        importer.readLong(from);
        importer.readLong(to);
    }

    @Override
    public int sizeofObject() {
        return 2*8;
    }
}
