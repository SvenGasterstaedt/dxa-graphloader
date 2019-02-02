package de.hhu.bsinfo.dxapp.data;

import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class WeightedEdge extends Edge {

    protected int weighted_value = 0;

    public WeightedEdge(){}

    public WeightedEdge(long p_id,long from,long to, int weighted_value){
        super(p_id,from,to);
        this.weighted_value = weighted_value;
    }

    @Override
    public void exportObject(Exporter exporter) {
        exporter.writeLong(from);
        exporter.writeLong(to);
        exporter.writeInt(weighted_value);
    }

    @Override
    public void importObject(Importer importer) {
        importer.readLong(from);
        importer.readLong(to);
        importer.readInt(weighted_value);
    }

    @Override
    public int sizeofObject() {
        return 20;
    }
}
