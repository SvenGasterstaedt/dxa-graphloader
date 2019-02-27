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
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class PeerVertexIDMap extends AbstractChunk implements ConcurrentMap<String, Long> {

    ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();

    public PeerVertexIDMap() {
    }

    public PeerVertexIDMap(long id) {
        setID(id);
    }

    public Long put(String string, Long aLong) {
        return map.put(string, aLong);
    }

    @Override
    public Long remove(Object o) {
        return map.remove(o);
    }

    @Override
    public synchronized void putAll(@NotNull Map<? extends String, ? extends Long> map) {
        this.map.putAll(map);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @NotNull
    @Override
    public Set<String> keySet() {
        return map.keySet();
    }

    @NotNull
    @Override
    public Collection<Long> values() {
        return map.values();
    }

    @NotNull
    @Override
    public Set<Entry<String, Long>> entrySet() {
        return map.entrySet();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    public boolean containsKey(Object o) {
        return map.containsKey(o);
    }

    @Override
    public boolean containsValue(Object o) {
        return map.containsValue(o);
    }

    @Override
    public Long get(Object o) {
        return map.get(o);
    }


    @Override
    public Long putIfAbsent(@NotNull String string, Long aLong) {
        return map.putIfAbsent(string, aLong);
    }

    @Override
    public boolean remove(@NotNull Object o, Object o1) {
        return map.remove(o, o1);
    }

    @Override
    public boolean replace(@NotNull String string, @NotNull Long aLong, @NotNull Long vertex1) {
        return map.replace(string, aLong, vertex1);
    }

    @Override
    public Long replace(@NotNull String string, @NotNull Long aLong) {
        return map.replace(string, aLong);
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(map.size());
        for (String string : keySet()) {
            p_exporter.writeString(string);
            p_exporter.writeLong(map.get(string));
        }
    }

    @Override
    public void importObject(Importer p_importer) {
        int size = 0;
        size = p_importer.readInt(size);
        if (size > 0) {
            for (int i = 0; i < size; i++) {
                String key = "";
                long v = ChunkID.INVALID_ID;
                key = p_importer.readString(key);
                v = p_importer.readLong(v);
                map.put(key, v);
            }
        }
    }

    @Override
    public int sizeofObject() {
        int size = 0;
        size += Integer.BYTES;
        for (String string : keySet()) {
            size += ObjectSizeUtil.sizeofString(string);
        }
        size += Long.BYTES * map.size();
        return size;
    }
}
