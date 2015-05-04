package idf;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.util.*;

import java.io.*;


/**
 * Splitter that reads a whole file as a single record
 * This is useful when you have a large number of files
 * each of which is a complete unit - for example XML Documents
 */
public class WholeFileInputFormat extends FileInputFormat<Text, Text> {

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split,
                       TaskAttemptContext context) {
        return new WholeFileReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

}