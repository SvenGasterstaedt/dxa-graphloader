package de.hhu.bsinfo.dxgraphloader.loader.data;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Distributable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.HashSet;

public class StringArray extends AbstractChunk implements Distributable {


    public String[] getKeys() {
        return keys;
    }

    private String[] keys;

    public StringArray(){
    }


    public StringArray(final long c_id){
        setID(c_id);
    }

    public StringArray(final HashSet<String> strings){
        keys = strings.toArray(new String[0]);
    }

    public StringArray(final String[] keys){
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
