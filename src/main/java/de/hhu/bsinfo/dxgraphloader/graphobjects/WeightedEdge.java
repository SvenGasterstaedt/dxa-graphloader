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

public class WeightedEdge extends Edge {

    private int m_weightedValue = 0;

    public WeightedEdge(long p_id){
        super(p_id);
    }

    public WeightedEdge(long p_id,long p_from,long p_to, int p_weightedValue){
        super(p_id,p_from,p_to);
        m_weightedValue = p_weightedValue;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeLong(from);
        p_exporter.writeLong(to);
        p_exporter.writeInt(m_weightedValue);
    }

    @Override
    public void importObject(Importer p_importer) {
        p_importer.readLong(from);
        p_importer.readLong(to);
        p_importer.readInt(m_weightedValue);
    }

    @Override
    public int sizeofObject() {
        return 20;
    }
}
