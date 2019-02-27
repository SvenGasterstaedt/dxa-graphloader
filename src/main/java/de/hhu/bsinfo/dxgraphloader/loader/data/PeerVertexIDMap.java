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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jetbrains.annotations.NotNull;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public final class PeerVertexIDMap extends AbstractChunk implements ConcurrentMap<String, Long> {

    private final ConcurrentHashMap<String, Long> m_map = new ConcurrentHashMap<>();

    @SuppressWarnings("WeakerAccess")
    public PeerVertexIDMap() {
    }

    @SuppressWarnings("unused")
    public PeerVertexIDMap(long p_id) {
        setID(p_id);
    }

    @Override
    public Long put(String p_string, Long p_long) {
        return m_map.put(p_string, p_long);
    }

    @Override
    public Long remove(Object p_obj) {
        return m_map.remove(p_obj);
    }

    @Override
    public synchronized void putAll(@NotNull Map<? extends String, ? extends Long> p_map) {
        m_map.putAll(p_map);
    }

    @Override
    public void clear() {
        m_map.clear();
    }

    @Override
    public @NotNull Set<String> keySet() {
        return m_map.keySet();
    }

    @Override
    public @NotNull Collection<Long> values() {
        return m_map.values();
    }

    @Override
    public @NotNull Set<Entry<String, Long>> entrySet() {
        return m_map.entrySet();
    }

    @Override
    public int size() {
        return m_map.size();
    }

    @Override
    public boolean isEmpty() {
        return m_map.isEmpty();
    }

    @Override
    public boolean containsKey(Object p_obj) {
        return m_map.containsKey(p_obj);
    }

    @Override
    public boolean containsValue(Object p_obj) {
        return m_map.containsValue(p_obj);
    }

    @Override
    public Long get(Object p_obj) {
        return m_map.get(p_obj);
    }

    @Override
    public Long putIfAbsent(@NotNull String p_string, Long p_long) {
        return m_map.putIfAbsent(p_string, p_long);
    }

    @Override
    public boolean remove(@NotNull Object p_obj, Object p_obj2) {
        return m_map.remove(p_obj, p_obj2);
    }

    @Override
    public boolean replace(@NotNull String p_string, @NotNull Long p_long, @NotNull Long p_vertex) {
        return m_map.replace(p_string, p_long, p_vertex);
    }

    @Override
    public Long replace(@NotNull String p_string, @NotNull Long p_long) {
        return m_map.replace(p_string, p_long);
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(m_map.size());
        for (String str : keySet()) {
            p_exporter.writeString(str);
            p_exporter.writeLong(m_map.get(str));
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
                m_map.put(key, v);
            }
        }
    }

    @Override
    public int sizeofObject() {
        int size = 0;
        size += Integer.BYTES;
        for (String str : keySet()) {
            size += ObjectSizeUtil.sizeofString(str);
        }
        size += Long.BYTES * m_map.size();
        return size;
    }
}
