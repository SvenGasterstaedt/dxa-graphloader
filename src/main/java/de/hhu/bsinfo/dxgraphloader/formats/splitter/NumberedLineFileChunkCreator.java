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
import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.hhu.bsinfo.dxgraphloader.loader.data.FileChunk;

public class NumberedLineFileChunkCreator extends LineFileChunkCreator {

    private static final Pattern M_PATTERN = Pattern.compile("\r\n|\r|\n");

    private long m_lineNumber = 0;

    public NumberedLineFileChunkCreator(String p_file, int p_chunkSize) {
        super(p_file, p_chunkSize);
    }

    @SuppressWarnings("Duplicates")
    @Override
    public FileChunk getNextChunk() {
        try {
            if (hasRemaining()) {
                if (remaining() < m_chunkSize) {
                    m_content = new byte[(int) remaining()]; //can be casted safely, because m_chunkSize is an Integer
                    m_file.read(m_content);

                    m_content = ByteBuffer.allocate(Long.BYTES + m_content.length).putLong(m_lineNumber).put(m_content)
                            .array();
                    Matcher matcher = M_PATTERN.matcher(new String(m_content));
                    while (matcher.find()) {
                        m_lineNumber++;
                    }
                    return new FileChunk(m_content);
                }

                m_content = new byte[m_chunkSize];
                m_file.read(m_content);

                byte[] remainingLine = new byte[0];
                if (hasRemaining()) {
                    remainingLine = (m_file.readLine() + '\n').getBytes();
                }
                m_content = ByteBuffer.allocate(Long.BYTES + m_content.length + remainingLine.length).putLong(
                        m_lineNumber).put(
                        m_content).put(remainingLine).array();
                Matcher matcher = M_PATTERN.matcher(new String(m_content));
                while (matcher.find()) {
                    m_lineNumber++;
                }
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
}
