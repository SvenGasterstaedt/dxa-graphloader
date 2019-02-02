package de.hhu.bsinfo.dxapp.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@SuppressWarnings("Duplicates")
public class MemoryMapLineReader {


    private MappedByteBuffer buffer;
    private static final int approx_line_length = 128;
    private char[] chars = new char[approx_line_length];


    public MemoryMapLineReader(File file) {
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            FileChannel fileChannel = randomAccessFile.getChannel();
            buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            randomAccessFile.close();
            fileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean ready() {
        return buffer.hasRemaining();
    }

    public String readLine() {
        if(buffer.hasRemaining()) {
            char c;
            int i = 0;
            do {
                c = (char) buffer.get();
                try {
                    chars[i] = c;
                } catch (ArrayIndexOutOfBoundsException e) {
                    chars = Arrays.copyOf(chars, chars.length + approx_line_length);
                    chars[i] = c;
                }
                i++;
            } while (c != '\n' && buffer.hasRemaining());
            return new String(chars, 0, i);
        }else{
            return null;
        }
    }

    public Stream<String> lines() {
        Iterator<String> iter = new Iterator<String>() {
            String nextLine = null;

            @Override
            public boolean hasNext() {
                if (nextLine != null) {
                    return true;
                } else {
                    nextLine = readLine();
                    return (nextLine != null);
                }
            }

            @Override
            public String next() {
                if (nextLine != null || hasNext()) {
                    String line = nextLine;
                    nextLine = null;
                    return line;
                } else {
                    throw new NoSuchElementException();
                }
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                iter, Spliterator.IMMUTABLE), true);
    }
}
