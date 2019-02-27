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

package de.hhu.bsinfo.dxgraphloader.graph.data;

import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class WeightedEdge extends Edge {

    protected int weighted_value = 0;

    public WeightedEdge(long id){
        super(id);
    }

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
