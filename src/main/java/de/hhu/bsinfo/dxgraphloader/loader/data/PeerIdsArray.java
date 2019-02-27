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
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public final class PeerIdsArray extends AbstractChunk {

    long[][] chunks = new long[0][0];


    public PeerIdsArray() {
    }

    public PeerIdsArray(long c_id) {
        setID(c_id);
    }

    public PeerIdsArray(int size) {
        chunks = new long[size][];
        for(int i = 0; i < chunks.length; i++){
            chunks[i] = new long[0];
        }
    }

    public boolean add(int i, long[] id) {
        if (i >= 0 && i < chunks.length) {
            chunks[i] = id;
            return true;
        }
        return false;
    }

    public long[] getArray(int peerPos) {
        if (peerPos >= 0 && peerPos < chunks.length) {
            return chunks[peerPos];
        }
        return null;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(chunks.length);
        for (long[] l : chunks)
            p_exporter.writeLongArray(l);
    }

    @Override
    public void importObject(Importer p_importer) {
        int size = 0;
        size = p_importer.readInt(size);
        chunks = new long[size][];
        for (int i = 0; i < size; i++) {
            chunks[i] = p_importer.readLongArray(chunks[i]);
        }
    }

    @Override
    public int sizeofObject() {
        int size = Integer.BYTES;
        for (long[] l : chunks) {
            size += ObjectSizeUtil.sizeofLongArray(l);
        }
        return size;
    }
}
