package designpattern.filtering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DistributedGrep {

    public static final String REGEX_KEY = "distributed.grep.filtering.regex";

    public static class GrepMapper extends Mapper<Object, Text, NullWritable, Text> {
        private Pattern pattern = null;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            pattern = Pattern.compile(context.getConfiguration().get(REGEX_KEY, ""));
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Matcher matcher = pattern.matcher(value.toString());
            if (matcher.find()) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        /* Create and configure a new MapReduce Job */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Distributed Grep");
        job.setJarByClass(DistributedGrep.class);

        /* *** Parse the arguments passed through the command line *** */
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        Path inputFile = null;
        Path outputFile = null;
        String regexp = null;
        if (remainingArgs.length != 3) {
            System.err.println("Usage: DistributedGrep <regex> <in> <out>");
            System.exit(2);
        } else {
            regexp = remainingArgs[0];
            inputFile = new Path(remainingArgs[1]);
            outputFile = new Path(remainingArgs[2]);
        }
        /* *** *** *** *** *** *** *** *** *** *** *** *** *** *** *** */

        job.setJarByClass(DistributedGrep.class);
        job.setMapperClass(GrepMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        job.getConfiguration().set(REGEX_KEY, regexp);
        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputFile);

        /* Wait for job termination */
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}