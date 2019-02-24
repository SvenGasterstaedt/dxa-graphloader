package de.hhu.bsinfo.dxgraphloader.util;

public class IDUtils {
    public static String shortToHexString(short s){
        return Integer.toHexString(s).substring(4).toUpperCase();
    }
}
