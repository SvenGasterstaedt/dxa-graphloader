/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxgraphloader.graphobjects;

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
    public void exportObject(Exporter p_exporter) {
        super.exportObject(p_exporter);
        p_exporter.writeString(label);
    }

    @Override
    public void importObject(Importer p_importer) {
        super.importObject(p_importer);
        p_importer.readString(label);
    }

    @Override
    public int sizeofObject() {
        return super.sizeofObject()+ ObjectSizeUtil.sizeofString(label);
    }

}
