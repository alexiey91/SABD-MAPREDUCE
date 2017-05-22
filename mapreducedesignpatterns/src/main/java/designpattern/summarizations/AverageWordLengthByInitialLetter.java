package designpattern.summarizations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class AverageWordLengthByInitialLetter {

    public static class AverageMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable length = new IntWritable(0);
        private Text initialLetter = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();

            /* Emit length by initial letter */
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                String word = itr.nextToken();
                initialLetter.set(word.substring(0,1));
                length.set(word.length());
                context.write(initialLetter, length);
            }
        }
    }

    public static class AverageReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {

        private FloatWritable average = new FloatWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            average.set(((float) sum / (float) count));
            context.write(key, average);
        }
    }

    public static void main(String[] args) throws Exception {

        /* Create and configure a new MapReduce Job */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average length by initial letter");
        job.setJarByClass(AverageWordLengthByInitialLetter.class);

        /* Map function */
        job.setMapperClass(AverageMapper.class);
        // if equal to the reduce output, can be omitted
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        /* A partitioner can be used to balance load among reducers */

        /* Reduce function */
        job.setReducerClass(AverageReducer.class);
        job.setNumReduceTasks(2);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        /* Set input and output files/directories using command line arguments */
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // if these files are different from text files, we can specify the format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        /* Wait for job termination */
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}