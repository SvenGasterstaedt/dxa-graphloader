package de.hhu.bsinfo.dxapp.formats;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Set;

public class SupportedFormats {
    private static HashMap<String, Class<? extends GraphFormat>> formats = new HashMap<>();

    static {
        formats.put("EDGEFILE", EdgeListFormat.class);
        formats.put("NEIGHBORLIST", GraphFormat.class);
        formats.put("ADJENCYMATRIX", GraphFormat.class);
        formats.put("GRAPHML", GraphFormat.class);
    }

    public static void addFormat(final String key, final Class<? extends GraphFormat> formatClass) {
        final String upper_key = key.toUpperCase();
        if (!formats.containsKey(upper_key))
            formats.put(upper_key, formatClass);
    }

    public static GraphFormat getFormat(final String key, final String[] files) {
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

    public static boolean isSupported(final String key) {
        return formats.containsKey(key.toUpperCase());
    }

    public static Set<String> supportedFormats() {
        return formats.keySet();
    }
}
