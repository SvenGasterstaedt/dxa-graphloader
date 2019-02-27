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

package de.hhu.bsinfo.dxgraphloader.loader.data;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.util.Arrays;

public final class LongArray extends AbstractChunk {

    private long[] ids;

    public LongArray(final long chunkId) {
        super();
        setID(chunkId);
    }

    public LongArray(final long[] ids) {
        this.ids = ids;
    }

    public LongArray(final Long[] ids) {
        this.ids = new long[ids.length];
        for(int i = 0; i < ids.length; i++){
            this.ids[i] = ids[i];
        }
    }

    public long getChunkID(int index) {
        if(index > -1 && index < ids.length){
            return ids[index];
        }
        return ChunkID.INVALID_ID;
    }

    public long[] getIds(){
        return ids;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(ids.length);
        for (long t_id : ids) {
            p_exporter.writeLong(t_id);
        }
    }

    @Override
    public void importObject(final Importer p_importer) {
        int size = 0;
        size = p_importer.readInt(size);
        ids = new long[size];
        Arrays.fill(ids, ChunkID.INVALID_ID);
        for (int i = 0; i < size; i++) {
            ids[i] = p_importer.readLong(ids[i]);
        }
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + Long.BYTES * ids.length;
    }
}
