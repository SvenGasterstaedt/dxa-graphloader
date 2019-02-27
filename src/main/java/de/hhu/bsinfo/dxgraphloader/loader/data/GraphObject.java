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

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.util.*;

public final class GraphObject extends AbstractChunk {

    HashMap<Short, Long> listPeerMaps = new HashMap<>();
    List<Short> peers = new ArrayList<>();

    public GraphObject(long id){
        setID(id);
    }

    public GraphObject(final List<Short> peers, final ChunkService chunkService) {
        this.peers = peers;
        for (short p : peers) {
            PeerVertexIDMap peerVertexMap = new PeerVertexIDMap();
            chunkService.create().create(p, peerVertexMap);
            chunkService.put().put(peerVertexMap);
            listPeerMaps.put(p, peerVertexMap.getID());
        }
    }

    public int getPeerSize(){
        return peers.size();
    }

    public int getPeerPos(short peer){
        return peers.indexOf(peer);
    }

    public List<Short> getPeers(){
        return peers;
    }

    public Map.Entry<Short, Long> getNodeAndMapId(int mod) {
        short p = peers.get(mod);
        return new AbstractMap.SimpleEntry<Short, Long>(p, listPeerMaps.get(p));
    }

    public short getNode(int mod) {
        return peers.get(Math.abs(mod));
    }

    public long getMap(short node) {
        return listPeerMaps.get(node);
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
