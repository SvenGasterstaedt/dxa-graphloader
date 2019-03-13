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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxgraphloader.GraphLoaderApp;
import de.hhu.bsinfo.dxgraphloader.loader.data.FileChunk;
import de.hhu.bsinfo.dxgraphloader.loader.formats.AbstractFileChunkCreator;

/**
 * <h1>LineFileChunkCreator</h1>
 * ChunkCreator, who skips lines for n bytes and stores them in a FileChunk.
 *
 * @author Sven Gasterstaedt
 * @version 1.0
 * @since 2019-03-15
 */
public class LineFileChunkCreator extends AbstractFileChunkCreator {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoaderApp.class.getSimpleName());

    int m_chunkSize;
    private long m_bytesTotal;
    byte[] m_content;

    RandomAccessFile m_file;

    public LineFileChunkCreator(String p_file, int p_chunkSize) {
        try {
            m_file = new RandomAccessFile(p_file, "r");
            m_bytesTotal = m_file.length();
            m_chunkSize = p_chunkSize;
            m_content = new byte[p_chunkSize];
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("IOException in AbstractFileChunkCreator");
        }
    }

    @Override
    public boolean hasRemaining() {
        try {
            return m_bytesTotal - m_file.getFilePointer() > 0;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    long remaining() throws IOException {
        return m_bytesTotal - m_file.getFilePointer();
    }

    @SuppressWarnings("Duplicates")
    @Override
    public FileChunk getNextChunk() {
        try {
            if (hasRemaining()) {
                if (remaining() < m_chunkSize) {
                    m_content = new byte[(int) remaining()]; //can be casted safely, because m_chunkSize is an Integer
                    m_file.read(m_content);
                    return new FileChunk(m_content);
                }

                m_content = new byte[m_chunkSize];
                m_file.read(m_content);

                byte[] remainingLine = new byte[0];
                if (hasRemaining()) {
                    remainingLine = (m_file.readLine() + '\n').getBytes();
                }
                ByteBuffer b =  ByteBuffer.allocate(m_content.length + remainingLine.length).put(m_content).put(
                        remainingLine);
                m_content = b.array();
                b = null;
                return new FileChunk(m_content);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    @Override
    public void setCycle(short p_cycle) {
        m_cycle = p_cycle;
        try {
            m_file.seek(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close(){
        try {
            m_file.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
