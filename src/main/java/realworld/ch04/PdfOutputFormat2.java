package realworld.ch04;

import com.itextpdf.text.Document;
import com.itextpdf.text.PageSize;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.pdf.PdfWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PdfOutputFormat2 extends FileOutputFormat<Text, Text> {
    TaskAttemptContext job;

    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        this.job = job;

        return new PdfOutputFormat2.PDFRecordWriter(job);
    }

    public class PDFRecordWriter extends RecordWriter<Text, Text> {
        private final Log log = LogFactory.getLog(PDFOutputFormat.PDFRecordWriter.class);

        TaskAttemptContext job;
        Path file;
        FileSystem fs;
        int i = 0;

        PDFRecordWriter(TaskAttemptContext job) {
            this.job = job;
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        }

        @Override
        public void write(Text key, Text value) throws IOException, InterruptedException {
            Configuration conf = job.getConfiguration();
            Path name = getDefaultWorkFile(job, null);
            String outfilepath = name.toString();
            String keyname = key.toString();
            Path file = new Path((outfilepath.substring(0, outfilepath.length() - 16)) + keyname + ".pdf");
            FileSystem fs = file.getFileSystem(conf);
            FSDataOutputStream fileOut = fs.create(file, false);

            try {
                Document document = new Document(PageSize.LETTER, 40, 40, 40, 40);
                PdfWriter.getInstance(document, fileOut);
                document.open();

                Paragraph p = new Paragraph(value.toString());
                document.add(p);

                document.close();
            } catch(Exception e) {
                log.error("Error - PDFOutputFormat");
            }
        }
    }
}
