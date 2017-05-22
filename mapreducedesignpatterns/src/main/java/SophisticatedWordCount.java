import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class SophisticatedWordCount {

    private static final String CONFIG_WORDCOUNT_SKIP_PATTERN = "wordcount.skip.patterns";
    private static final String CONFIG_WORDCOUNT_CASE_SENSITIVE = "wordcount.case.sensitive";


    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private boolean caseSensitive;
        private Set<String> patternsToSkip = new HashSet<String>();

        private Configuration conf;
        private BufferedReader fis;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
            caseSensitive = conf.getBoolean(CONFIG_WORDCOUNT_CASE_SENSITIVE, true);
            if (conf.getBoolean(CONFIG_WORDCOUNT_SKIP_PATTERN, false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();

            /* Remove words in the patten list */
            for (String pattern : patternsToSkip) {
                line = line.replaceAll(pattern, "");
            }

            /* Emit words */
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class CustomPartitioner extends Partitioner<Text, IntWritable> {

        @Override
        public int getPartition(Text text, IntWritable intWritable, int numReducers) {

            String tv = text.toString();
            int first = (int) tv.toLowerCase().charAt(0);

            return (first - 'j') < 0 ? (0 % numReducers) : (1 % numReducers);

        }

    }

    public static void main(String[] args) throws Exception {

        /* Create the configuration file */
        Configuration conf = new Configuration();

        /* *** Parse the arguments passed through the command line *** */
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        Path inputFile = null;
        Path outputFile = null;
        boolean userSkipFile = false;
        Path patternToSkipFile = null;
        if (!(remainingArgs.length != 2 || remainingArgs.length != 4)) {
            System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
            System.exit(2);
        } else {
            inputFile = new Path(remainingArgs[0]);
            outputFile = new Path(remainingArgs[1]);
            if (remainingArgs.length == 4 && "-skip".equals(remainingArgs[2])) {
                patternToSkipFile = new Path(remainingArgs[3]);
                userSkipFile = true;
            }
        }
        /* *** *** *** *** *** *** *** *** *** *** *** *** *** *** *** */

        /* Create and configure a new MapReduce Job */
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(SophisticatedWordCount.class);

        /* Map function */
        job.setMapperClass(TokenizerMapper.class);
        // if equal to the reduce output, can be omitted
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        /* Combine function */
        job.setCombinerClass(IntSumReducer.class);

        /* Partition function */
        job.setPartitionerClass(CustomPartitioner.class);

        /* Reduce function */
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(2);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /* Add resources to the job */
        if (userSkipFile) {
            job.addCacheFile(patternToSkipFile.toUri());
            job.getConfiguration().setBoolean(CONFIG_WORDCOUNT_SKIP_PATTERN, true);
        }

        /* Set input and output files/directories using command line arguments */
        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputFile);
        // if these files are different from text files, we can specify the format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        /* Wait for job termination */
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}