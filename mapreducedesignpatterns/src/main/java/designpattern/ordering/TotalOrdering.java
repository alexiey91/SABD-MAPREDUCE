package designpattern.ordering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;

public class TotalOrdering {

    public static class AnalyzePhaseMapper extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            outkey.set(value.toString());
            context.write(outkey, value);
        }

    }

    public static class OrderingPhaseReducer extends Reducer<Text, Text, Text, NullWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text t : values) {
                context.write(t, NullWritable.get());
            }

        }

    }


    public static void main(String[] args) throws Exception {

        /* Create the configuration file and set the file (input, partition, state, output) */
        Configuration conf = new Configuration();
        Path inputPath = new Path(args[0]);
        Path partitionFile = new Path(args[1] + "_partitions.lst");
        Path outputStage = new Path(args[1] + "_staging");
        Path outputOrder = new Path(args[1]);

        /* **** Job #1: Analyze phase **** */
        /* Sample the data set to determine the slice boundaries */
        Job sampleJob = Job.getInstance(conf, "TotalOrderSortingStage");
        sampleJob.setJarByClass(TotalOrdering.class);

        /* Map: samples data; Reduce: identity function */
        sampleJob.setMapperClass(AnalyzePhaseMapper.class);
        sampleJob.setNumReduceTasks(0);
        sampleJob.setOutputKeyClass(Text.class);
        sampleJob.setOutputValueClass(Text.class);

        /* Set input and output files */
        TextInputFormat.setInputPaths(sampleJob, inputPath);
        sampleJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(sampleJob, outputStage);

        /* Submit the job and get completion code. */
        int code = sampleJob.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {

            /* **** Job #2: Ordering phase **** */
            Job orderJob = Job.getInstance(conf, "TotalOrderSortingStage");
            orderJob.setJarByClass(TotalOrdering.class);

            /* Map: identity function outputs the key/value pairs in the SequenceFile */
            orderJob.setMapperClass(Mapper.class);

            /* Reduce: identity function (the important data is the key, value is null) */
            orderJob.setReducerClass(OrderingPhaseReducer.class);
            orderJob.setNumReduceTasks(10);

            /* Route key/value pairs to reducers, using the splits previously computed
               the TotalOlderPartitioner already implements this functionality */
            orderJob.setPartitionerClass(TotalOrderPartitioner.class);
            TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(), partitionFile);
            orderJob.setOutputKeyClass(Text.class);
            orderJob.setOutputValueClass(Text.class);


            /* Set input and output files: the input is the previous job's output */
            orderJob.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(orderJob, outputStage);

            TextOutputFormat.setOutputPath(orderJob, outputOrder);
            // Set the separator to an empty string
            orderJob.getConfiguration().set("mapred.textoutputformat.separator", "");

            /* Use the InputSampler to go through the output of the previous
                job, sample it, and create the partition file */
            InputSampler.writePartitionFile(orderJob, new InputSampler.RandomSampler(.3, 10));

            /* Submit the job and get completion code. */
            code = orderJob.waitForCompletion(true) ? 0 : 2;
        }

        /* Clean up the partition file and the staging directory */
        FileSystem.get(new Configuration()).delete(partitionFile, false);
        FileSystem.get(new Configuration()).delete(outputStage, true);

        /* Wait for job termination */
        System.exit(code);

    }

}
