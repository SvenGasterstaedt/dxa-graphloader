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
import de.hhu.bsinfo.dxutils.serialization.Distributable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.HashSet;

public final class KeyArray extends AbstractChunk implements Distributable {


    public String[] getKeys() {
        return keys;
    }

    private String[] keys;

    public KeyArray(){
    }


    public KeyArray(final long c_id){
        setID(c_id);
    }

    public KeyArray(final HashSet<String> strings){
        keys = strings.toArray(new String[0]);
    }

    public KeyArray(final String[] keys){
        this.keys = keys;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(keys.length);
        for(String s: keys) {
            p_exporter.writeString(s);
        }
    }

    @Override
    public void importObject(final Importer p_importer) {
        int size = 0;
        size = p_importer.readInt(size);
        keys = new String[size];
        for(int i = 0; i < size;i++){
            keys[i] = p_importer.readString(keys[i]);
        }
    }

    @Override
    public int sizeofObject() {
        int size = Integer.BYTES;
        for(String s:keys){
            size += ObjectSizeUtil.sizeofString(s);
        }
        return size;
    }
}
