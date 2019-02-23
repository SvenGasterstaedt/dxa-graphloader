package de.hhu.bsinfo.dxgraphloader.loader.data;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.util.*;

public class DistributedObjectTable extends AbstractChunk {

    HashMap<Short, Long> listPeerMaps = new HashMap<>();
    List<Short> peers;

    public DistributedObjectTable(long id){
        setID(id);
    }

    public DistributedObjectTable(final List<Short> peers, final ChunkService chunkService) {
        this.peers = peers;
        for (short p : peers) {
            PeerVertexMap peerVertexMap = new PeerVertexMap();
            chunkService.create().create(p, peerVertexMap);
            listPeerMaps.put(p, peerVertexMap.getID());
        }
    }

    public Map.Entry<Short, Long> getNodeAndMapId(int mod) {
        short p = peers.get(mod);
        return new AbstractMap.SimpleEntry<Short, Long>(p, listPeerMaps.get(p));
    }

    public short getNode(int mod) {
        return peers.get(mod);
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(listPeerMaps.size());
        for (Map.Entry<Short, Long> entry : listPeerMaps.entrySet()) {
            p_exporter.writeShort(entry.getKey());
            p_exporter.writeLong(entry.getValue());
        }
        p_exporter.writeInt(peers.size());
        for (short s : peers) {
            p_exporter.writeShort(s);
        }
    }

    @Override
    public void importObject(Importer p_importer) {
        int size = 0;
        size = p_importer.readInt(size);
        short s = 0;
        long l = 0;
        for (int i = 0; i < size; i++) {
            s = p_importer.readShort(s);
            l = p_importer.readLong(l);

            listPeerMaps.put(s,l);
        }
        size = p_importer.readInt(size);
        peers = new ArrayList<>(size);
        for(int i = 0; i < size; i++){
            s = p_importer.readShort(s);
            peers.add(s);
        }
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + Short.BYTES * listPeerMaps.size() + Long.BYTES * listPeerMaps.size() + Integer.BYTES + Short.BYTES * peers.size();
    }
}
