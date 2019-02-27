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
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public final class FileChunk extends AbstractChunk {


    private byte[] data;
    private boolean hasNext = true;


    public FileChunk(final long p_id) {
        this.setID(p_id);
    }

    public FileChunk(final byte[] fileData) {
        data = fileData;
        setID(ChunkID.INVALID_ID);
    }

    public byte[] getContents() {
        return data;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeByteArray(data);
        p_exporter.writeBoolean(hasNext);
    }


    @Override
    public void importObject(final Importer p_importer) {
        data = p_importer.readByteArray(data);
        hasNext = p_importer.readBoolean(hasNext);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofByteArray(data) + ObjectSizeUtil.sizeofBoolean();
    }
}