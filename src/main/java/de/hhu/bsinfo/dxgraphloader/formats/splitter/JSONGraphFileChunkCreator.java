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
import de.hhu.bsinfo.dxgraphloader.loader.formats.AbstractFileChunkCreator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class JSONGraphFileChunkCreator extends AbstractFileChunkCreator {

    private JsonReader m_reader;

    //cycles
    private static final short EDGES = 0;
    private static final short VERTICES = 1;

    private int m_chunkSize;
    private String m_filepath;

    public JSONGraphFileChunkCreator(final String p_filepath, final int p_chunkSize) {
        m_filepath = p_filepath;
        m_chunkSize = p_chunkSize;
    }

    @Override
    public boolean hasRemaining() {
        try {
            return m_reader.hasNext();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public FileChunk getNextChunk() {
        try {
            StringBuilder builder = new StringBuilder();
            while (m_reader.hasNext() && builder.length() * Character.BYTES < m_chunkSize) {
                m_reader.beginObject();
                while (m_reader.hasNext()) {
                    if (m_cycle == VERTICES) {
                        if ("id".equals(m_reader.nextName())) {
                            builder.append(m_reader.nextString()).append('\n');
                        } else {
                            m_reader.skipValue();
                        }
                    } else if (m_cycle == EDGES) {
                        String name = m_reader.nextName();
                        if ("source".equals(name)) {
                            builder.append(m_reader.nextString()).append('\t');
                        } else if ("target".equals(name)) {
                            builder.append(m_reader.nextString()).append('\n');
                        } else {
                            m_reader.skipValue();
                        }
                    }
                }
                m_reader.endObject();
            }
            return new FileChunk(builder.toString().getBytes());
        } catch (
                IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void setUp() throws IOException {
        if (m_reader != null) {
            m_reader.close();
        }
        BufferedReader buffReader = new BufferedReader(new FileReader(new File(m_filepath)));
        m_reader = new Gson().newJsonReader(buffReader);
        m_reader.beginObject();
        if ("graphs".equals(m_reader.nextName())) {
            m_reader.beginArray();
            while (m_reader.hasNext()) {
                m_reader.beginObject();
                while (m_reader.hasNext()) {
                    if (m_cycle == VERTICES) {
                        if ("nodes".equals(m_reader.nextName())) {
                            m_reader.beginArray();
                            return;
                        } else {
                            m_reader.skipValue();
                        }
                    }
                    if (m_cycle == EDGES) {
                        if ("edges".equals(m_reader.nextName())) {
                            m_reader.beginArray();
                            return;
                        } else {
                            m_reader.skipValue();
                        }
                    }

                }
            }
        }
    }

    @Override
    public void setCycle(short p_cycle) {
        m_cycle = p_cycle;
        try {
            setUp();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
