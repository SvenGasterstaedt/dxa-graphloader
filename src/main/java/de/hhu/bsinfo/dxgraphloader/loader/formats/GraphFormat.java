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

package de.hhu.bsinfo.dxgraphloader.loader.formats;

public class GraphFormat {
    private AbstractFileChunkCreator m_fileChunkCreator;
    private Class<? extends AbstractGraphFormatReader> m_formatReader;
    private String[] m_files;

    private short m_cycles;

    public GraphFormat(final String... p_files) {
        m_files = p_files;
    }

    public AbstractFileChunkCreator getFileChunkCreator(short p_cycle) {
        m_fileChunkCreator.setCycle(p_cycle);
        return m_fileChunkCreator;
    }

    public Class<? extends AbstractGraphFormatReader> getGraphFormatReader() {
        return m_formatReader;
    }

    @SuppressWarnings("WeakerAccess")
    public void setGraphFormatReader(Class<? extends AbstractGraphFormatReader> p_formatReader) {
        m_formatReader = p_formatReader;
    }

    @SuppressWarnings("WeakerAccess")
    public void setFileChunkCreator(AbstractFileChunkCreator p_fileChunkCreator) {
        m_fileChunkCreator = p_fileChunkCreator;
    }

    public short getCycles() {
        return m_cycles;
    }

    public void setCycles(short p_cycles) {
        m_cycles = p_cycles;
    }
}


