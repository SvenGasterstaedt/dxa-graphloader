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

package de.hhu.bsinfo.dxgraphloader.formats.splitter;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import de.hhu.bsinfo.dxgraphloader.loader.data.FileChunk;
import de.hhu.bsinfo.dxgraphloader.loader.formats.FileChunkCreator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class JSONGraphFileChunkCreator extends FileChunkCreator {

    JsonReader reader;

    short cycle = 0;

    //cycles
    private static final short EDGES = 0;
    private static final short VERTICES = 1;

    int chunkSize;
    String filePath;

    public JSONGraphFileChunkCreator(final String filePath, final int chunkSize) {
        this.filePath = filePath;
        this.chunkSize = chunkSize;
    }

    @Override
    public boolean hasRemaining() {
        try {
            return reader.hasNext();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public FileChunk getNextChunk() {
        try {
            StringBuilder builder = new StringBuilder();
            while (reader.hasNext() && builder.length() * Character.BYTES < chunkSize) {
                reader.beginObject();
                while (reader.hasNext()) {
                    if (cycle == VERTICES) {
                        if (reader.nextName().equals("id")) {
                            builder.append(reader.nextString()).append('\n');
                        } else {
                            reader.skipValue();
                        }
                    } else if (cycle == EDGES) {
                        String name = reader.nextName();
                        if (name.equals("source")) {
                            builder.append(reader.nextString()).append('\t');
                        } else if (name.equals("target")) {
                            builder.append(reader.nextString()).append('\n');
                        } else {
                            reader.skipValue();
                        }
                    }
                }
                reader.endObject();
            }
            return new FileChunk(builder.toString().getBytes());
        } catch (
                IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void setUp() throws IOException {
        if (reader != null) {
            reader.close();
        }
        BufferedReader buffReader = new BufferedReader(new FileReader(new File(filePath)));
        this.reader = new Gson().newJsonReader(buffReader);
        reader.beginObject();
        if (reader.nextName().equals("graphs")) {
            reader.beginArray();
            while (reader.hasNext()) {
                reader.beginObject();
                while (reader.hasNext()) {
                    if (cycle == VERTICES) {
                        if (reader.nextName().equals("nodes")) {
                            reader.beginArray();
                            return;
                        } else {
                            reader.skipValue();
                        }
                    }
                    if (cycle == EDGES) {
                        if (reader.nextName().equals("edges")) {
                            reader.beginArray();
                            return;
                        } else {
                            reader.skipValue();
                        }
                    }

                }
            }
        }
    }

    @Override
    public int getApproxChunkAmount() {
        return 0;
    }

    @Override
    public void setCycle(short cycle) {
        this.cycle = cycle;
        try {
            setUp();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
