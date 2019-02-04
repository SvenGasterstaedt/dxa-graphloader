package de.hhu.bsinfo.dxapp.formats;

import java.util.HashMap;

public class SupportedFormats {
    public static HashMap<String,Class<? extends GraphFormat>> formats = new HashMap<>();
    static {
        formats.put("EDGEFILE", GraphFormat.class);
        formats.put("NEIGHBORLIST", GraphFormat.class);
        formats.put("ADJENCYMATRIX", GraphFormat.class);
        formats.put("GRAPHML", GraphFormat.class);
    }
}
