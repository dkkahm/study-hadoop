package realworld.ch04;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PDFMapper extends Mapper<Text, PDFWritable, Text, PDFWritable> {
    private static final Log log = LogFactory.getLog(PDFMapper.class);
    String dirName = null;
    String fileName = null;

    @Override
    protected void map(Text key, PDFWritable value, Context context) throws IOException, InterruptedException {
        try {
            context.write(key, value);
        } catch(Exception e) {
            log.info(e);
        }
    }
}
