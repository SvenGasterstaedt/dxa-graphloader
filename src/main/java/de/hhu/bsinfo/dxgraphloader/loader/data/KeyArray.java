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

import java.util.HashSet;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Distributable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public final class KeyArray extends AbstractChunk implements Distributable {

    private String[] m_keys;

    @SuppressWarnings("unused")
    public KeyArray() {
    }

    public String[] getKeys() {
        return m_keys;
    }

    public KeyArray(final long p_id) {
        setID(p_id);
    }

    public KeyArray(final HashSet<String> p_keys) {
        m_keys = p_keys.toArray(new String[0]);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_keys.length);
        for (String s : m_keys) {
            p_exporter.writeString(s);
        }
    }

    @Override
    public void importObject(final Importer p_importer) {
        int size = 0;
        size = p_importer.readInt(size);
        m_keys = new String[size];
        for (int i = 0; i < size; i++) {
            m_keys[i] = p_importer.readString(m_keys[i]);
        }
    }

    @Override
    public int sizeofObject() {
        int size = Integer.BYTES;
        for (String s : m_keys) {
            size += ObjectSizeUtil.sizeofString(s);
        }
        return size;
    }
}
