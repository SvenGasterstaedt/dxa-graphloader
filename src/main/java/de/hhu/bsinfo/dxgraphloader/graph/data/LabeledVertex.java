package de.hhu.bsinfo.dxgraphloader.graph.data;

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
        super.exportObject(exporter);
        exporter.writeString(label);
    }

    @Override
    public void importObject(Importer importer) {
        super.importObject(importer);
        importer.readString(label);
    }

    @Override
    public int sizeofObject() {
        return super.sizeofObject()+ ObjectSizeUtil.sizeofString(label);
    }

}
