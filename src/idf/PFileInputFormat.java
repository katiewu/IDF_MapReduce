package idf;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class PFileInputFormat extends FileInputFormat<Text, Text>{

	@Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        PFileRecordReader reader = new PFileRecordReader();
        reader.initialize(inputSplit, context);
        return reader;
    }

}
