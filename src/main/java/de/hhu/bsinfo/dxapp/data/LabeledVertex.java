package de.hhu.bsinfo.dxapp.data;

import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class LabeledVertex extends Vertex{

    private String label;

    public LabeledVertex(String label){
        super();
        this.label = label;
    }

    public LabeledVertex(long p_id, String label){
        super(p_id);
        this.label = label;
    }

    public LabeledVertex(long p_id, String label, int neighbor_count) {
        super(p_id, neighbor_count);
        this.label = label;
    }

    @Override
    public void exportObject(Exporter exporter) {
        exporter.writeLongArray(edges);
        exporter.writeString(label);
    }

    @Override
    public void importObject(Importer importer) {
        importer.readLongArray(edges);
        importer.readString(label);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofLongArray(edges)+ ObjectSizeUtil.sizeofString(label);
    }

}
