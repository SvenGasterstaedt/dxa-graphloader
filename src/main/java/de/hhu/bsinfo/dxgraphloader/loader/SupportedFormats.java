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

import de.hhu.bsinfo.dxgraphloader.formats.EdgeList;
import de.hhu.bsinfo.dxgraphloader.loader.formats.GraphFormat;
import de.hhu.bsinfo.dxgraphloader.formats.JSONGraph;
import de.hhu.bsinfo.dxgraphloader.formats.NeighborList;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Set;

public final class SupportedFormats{
    private HashMap<String, Class<? extends GraphFormat>> formats = new HashMap<>();

    public SupportedFormats(){
        formats.put("EDGE", EdgeList.class);
        formats.put("NEIGHBOR", NeighborList.class);
        formats.put("JSON", JSONGraph.class);
        formats.put("GRAPHML", GraphFormat.class);
    }

    public void addFormat(final String key, final Class<? extends GraphFormat> formatClass) {
        final String upper_key = key.toUpperCase();
        if (!formats.containsKey(upper_key))
            formats.put(upper_key, formatClass);
    }

    public GraphFormat getFormat(final String key, final String[] files) {
        final GraphFormat graphFormat;
        try {
            Constructor constructor = formats.get(key).getDeclaredConstructor( files.getClass());
            graphFormat = (GraphFormat)constructor.newInstance(new Object[]{files});
            return graphFormat;
        } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException | InstantiationException e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean isSupported(final String key) {
        return formats.containsKey(key.toUpperCase());
    }

    public Set<String> supportedFormats() {
        return formats.keySet();
    }
}
