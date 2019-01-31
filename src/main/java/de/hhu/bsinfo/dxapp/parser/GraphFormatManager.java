package de.hhu.bsinfo.dxapp.parser;


import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

public class GraphFormatManager {
    private HashMap<String, Class<? extends GraphFormatParser>> formats = new HashMap();
    private static final String default_path = "";


    public GraphFormatManager() {
        addFormat("edgelist",EdgeListParser.class);
    }

    public GraphFormatManager(String formats_file) {
    }

    public void addFormat(String key, Class<? extends GraphFormatParser> parser) {
        formats.put(key, parser);
    }

    public GraphFormatParser getFormat(String format, byte[] chunk) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return formats.get(format).getDeclaredConstructor(Byte.class).newInstance(chunk);
    }
}
