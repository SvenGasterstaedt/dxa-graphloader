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

package de.hhu.bsinfo.dxgraphloader.loader;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Set;

import de.hhu.bsinfo.dxgraphloader.formats.EdgeList;
import de.hhu.bsinfo.dxgraphloader.formats.JSONGraph;
import de.hhu.bsinfo.dxgraphloader.formats.NeighborList;
import de.hhu.bsinfo.dxgraphloader.loader.formats.GraphFormat;

public final class SupportedFormats {
    private HashMap<String, Class<? extends GraphFormat>> m_formats = new HashMap<>();

    SupportedFormats() {
        m_formats.put("EDGE", EdgeList.class);
        m_formats.put("NEIGHBOR", NeighborList.class);
        m_formats.put("JSON", JSONGraph.class);
        m_formats.put("GRAPHML", GraphFormat.class);
    }

    @SuppressWarnings("unused")
    public void addFormat(final String p_key, final Class<? extends GraphFormat> p_formatClass) {

        final String upperKey = p_key.toUpperCase();
        if (!m_formats.containsKey(upperKey)) {

            m_formats.put(upperKey, p_formatClass);
        }
    }

    GraphFormat getFormat(final String p_key, final String[] p_files) {

        final GraphFormat graphFormat;
        try {

            Constructor constructor = m_formats.get(p_key).getDeclaredConstructor(p_files.getClass());
            graphFormat = (GraphFormat) constructor.newInstance(new Object[] {p_files});
            return graphFormat;
        } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException |InstantiationException e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean isSupported(final String p_key) {
        return m_formats.containsKey(p_key.toUpperCase());
    }

    public Set<String> supportedFormats() {
        return m_formats.keySet();
    }
}
