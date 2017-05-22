package designpattern.partitioning;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class PartitionDataByYear {

    private static int CONFIG_INITIAL_YEAR = 2010;
    private static int CONFIG_FINAL_YEAR = 2016;

    public static class DateMapper extends Mapper<Object, Text, IntWritable, Text> {

        private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd");
        private IntWritable outkey = new IntWritable();

        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String strDate = value.toString();

            Calendar cal = Calendar.getInstance();
            try {
                cal.setTime(frmt.parse(strDate));
                outkey.set(cal.get(Calendar.YEAR));
                context.write(outkey, value);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

    }

    public static class DatePartitioner extends Partitioner<IntWritable, Text> {

        public int getPartition(IntWritable key, Text value, int numPartitions) {
            return (key.get() - CONFIG_INITIAL_YEAR) % numPartitions;
        }

    }

    public static class DateReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t : values) {
                context.write(t, NullWritable.get());
            }
        }

    }


    public static void main(String[] args) throws Exception{

        /* Create and configure a new MapReduce Job */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Date Partitioning");
        job.setJarByClass(PartitionDataByYear.class);

        /* Map function */
        job.setMapperClass(DateMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        /* Partition function */
        job.setPartitionerClass(DatePartitioner.class);

        /* Reduce function */
        job.setReducerClass(DateReducer.class);
        job.setNumReduceTasks((CONFIG_FINAL_YEAR - CONFIG_INITIAL_YEAR + 1));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

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


