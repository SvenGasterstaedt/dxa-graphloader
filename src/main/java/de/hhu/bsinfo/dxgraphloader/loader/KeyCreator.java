package de.hhu.bsinfo.dxgraphloader.loader;

import java.util.ArrayList;
import java.util.List;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class KeyCreator extends AbstractChunk {
    private List<Short> m_peers;
    private static final int M_LENGHT = 10;

    public KeyCreator() {
    }

    public KeyCreator(final List<Short> p_peers) {
        m_peers = new ArrayList<>(p_peers);
    }

    public long createStrKey(final String p_alphabeticKey) throws Exception {
        String keyString = p_alphabeticKey.toLowerCase();
        if (keyString.length() > M_LENGHT || !keyString.chars().allMatch(Character::isAlphabetic)) {
            throw new Exception("Invalid Key String!");
        }
        long key = 0;
        for (int i = 0; i < keyString.length(); i++) {
            key += Math.pow(27, M_LENGHT - i - 1) * (1 + keyString.charAt(i) - 'a');
        }
        short targetPeer = (short) (key % m_peers.size());
        return key + targetPeer << 48;
    }

    public long createNumKey(final String p_numericKey) throws Exception {
        if (!p_numericKey.chars().allMatch(Character::isDigit)) {
            throw new Exception("Invalid Key String!");
        }
        long key = Long.valueOf(p_numericKey);
        if (key < 0 || key > 4398046509056L) {
            throw new NumberFormatException("Number to small or to big");
        }
        short targetPeer = m_peers.get((short) (key % m_peers.size()));
        return key + ((long) targetPeer << 48);
    }

    public List<Short> getPeers() {
        return m_peers;
    }

    public static short getPeer(final long p_key) {
        return (short) (p_key >>> 48);
    }

    public int peerIndex(final long p_key){
        return getPeerIndex(getPeer(p_key));
    }

    public int getPeerIndex(final short p_peer) {
        return m_peers.indexOf(p_peer);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_peers.size());
        for (short shrt : m_peers) {
            p_exporter.writeShort(shrt);
        }
    }

    @Override
    public void importObject(final Importer p_importer) {
        int size = 0;
        size = p_importer.readInt(size);
        m_peers = new ArrayList<>(size);
        short shrt = 0;
        for (int i = 0; i < size; i++) {
            shrt = p_importer.readShort(shrt);
            m_peers.add(shrt);
        }
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + m_peers.size() * Short.BYTES;
    }
}
