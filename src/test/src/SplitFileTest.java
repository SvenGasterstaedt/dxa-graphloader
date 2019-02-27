import de.hhu.bsinfo.dxgraphloader.formats.splitter.SkippingFileChunkCreator;
import org.assertj.core.api.Java6Assertions;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;

public class SplitFileTest {

  /*  @Test
    public void TestIntegrityOfSplittedFiles() throws Exception {
        File file = new File("src/test/data/dataset1.txt");
        File result = new File("out.txt");
        FileOutputStream out_stream = new FileOutputStream(result);
        SkippingLineFileChunkCreator skipper = new SkippingLineFileChunkCreator(file);
        byte[] chunk;
        while (skipper.ready()) {
            chunk = skipper.getNextChunk();
            out_stream.write(chunk);
        }
        Java6Assertions.assertThat(result).hasSameContentAs(file);
    }
*/
    @Test
    public void TestIntegrityOfSplittedFiles3() throws Exception {
        File file = new File("src/test/data/dataset2.txt");
        File result = new File("out2.txt");
        FileOutputStream out_stream = new FileOutputStream(result);
        SkippingFileChunkCreator skipper = new SkippingFileChunkCreator(file, Integer.MAX_VALUE/1024);
        byte[] chunk;
        while (skipper.ready()) {
            chunk = skipper.getNextChunk();
            out_stream.write(chunk);
        }
        Java6Assertions.assertThat(result).hasSameContentAs(file);
    }
}
