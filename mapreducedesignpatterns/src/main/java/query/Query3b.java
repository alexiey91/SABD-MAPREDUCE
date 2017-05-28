package query;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import util.FilmRating;


import java.io.IOException;
import java.util.Calendar;

/**
 * Created by alessandro on 26/05/2017.
 */
public class Query3b {

    public static abstract class GenericHierarchyMapper extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable outKey = new IntWritable();
        private Text outValue = new Text();
        private final String valuePrefix;

        protected GenericHierarchyMapper(String valuePrefix) {
            this.valuePrefix = valuePrefix;
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String movieId;
            String content= new String();
            String line = value.toString();
            String[] parts = line.split(",");

            if(valuePrefix.equals("R")) {
                Calendar bottomLimit = Calendar.getInstance();
                bottomLimit.set(2014,Calendar.APRIL,1);
                Calendar temp1 = Calendar.getInstance();
                temp1.setTimeInMillis(Long.parseLong(parts[3])*1000);
                Calendar middleLimit = Calendar.getInstance();
                middleLimit.set(2015,Calendar.MARCH,31);
                /*if(temp1.getTime().compareTo(temp.getTime())<0)
                    System.out.println("VECCHIO RATING"+temp1.getTime());*/
                if(temp1.getTime().compareTo(bottomLimit.getTime())<0)
                    return;
                else if (temp1.getTime().compareTo(middleLimit.getTime())>0){
                    content = "L"+parts[2];
                }
                else{
                    content = "P"+parts[2];
                }
                movieId = parts[1];


            }else{
                movieId = parts[0];
                for(int j = 1;j<parts.length-1;j++)
                    content += parts[j];//title
            }
            outKey.set(Integer.parseInt(movieId));
            outValue.set(valuePrefix + content);
            context.write(outKey, outValue);

        }
    }
    public static class DatePartitioner extends Partitioner<IntWritable, Text> {

        public int getPartition(IntWritable key, Text value, int numPartitions) {

            return (key.get() ) % numPartitions;
        }
    }
    public static class RatingMapper extends GenericHierarchyMapper {
        public RatingMapper() {
            super("R");
        }
    }

    public static class FilmMapper extends GenericHierarchyMapper {
        public FilmMapper() {
            super("F");
        }
    }

    public static class TopicHierarchyReducer extends
            //Reducer<IntWritable, Text, Text, NullWritable> {
            Reducer<IntWritable, Text, Text, Text> {

        public enum ValueType { RATING, FILM , UNKNOWN, PREV ,LAST}
        //private Gson gson = new Gson();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            FilmRating films = new FilmRating();
            for (Text t : values) {

                String value = t.toString();
                if (ValueType.FILM.equals(discriminate(value))){
                    films.setTitle(key+":"+getContent(value));
                } else if (ValueType.RATING.equals(discriminate(value))){
                    if(ValueType.PREV.equals(discriminateRating(value)))
                    films.addRatingPrev(Double.parseDouble(getRatingContent(value)));
                    else films.addRatingLast(Double.parseDouble(getRatingContent(value)));

                }

            }

             context.write(new Text(films.getRatingAvgPrev().toString()),new Text(films.getTitle()+":"+films.getRatingAvgLast()));

        }



        private ValueType discriminate(String value){

            char d = value.charAt(0);
            switch (d){
                case 'R':
                    return ValueType.RATING;
                case 'F':
                    return ValueType.FILM;
            }

            return ValueType.UNKNOWN;
        }

        private String getContent(String value){
            return value.substring(1);
        }

        private String getRatingContent(String value){
            return value.substring(2);
        }

        private ValueType discriminateRating(String value){

            char d = value.charAt(1);
            switch (d){
                case 'L':
                    return ValueType.LAST;
                case 'P':
                    return ValueType.PREV;
            }

            return ValueType.UNKNOWN;
        }


    }

    public static class OrderingPhaseReducer extends Reducer<Text, Text, Text, NullWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text t : values) {
                context.write(new Text(key.toString()+","+t), NullWritable.get());
            }

        }

    }
    public static void main(String[] args) throws Exception {

        /* Create and configure a new MapReduce Job */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query3b");
        job.setJarByClass(Query3b.class);
        Path partitionFile = new Path(args[2] + "_partitions.lst");
        Path outputStage = new Path(args[2] + "_staging");
        Path outputOrder = new Path(args[2]);


        /* Map function, from multiple input file
         * arg[0] rating
         * arg[1] film*/
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FilmMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        /* Reduce function */
        job.setReducerClass(TopicHierarchyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);


        //job.setPartitionerClass(DatePartitioner.class);

        /* Set output files/directories using command line arguments */
        //FileOutputFormat.setOutputPath(job, new Path(args[2]));
        //FileOutputFormat.setOutputPath(job,outputStage);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, outputStage);
        int code = job.waitForCompletion(true) ? 0 : 1;

        /* Wait for job termination */
        if (code == 0) {

            /* **** Job #2: Ordering phase **** */
            Job orderJob = Job.getInstance(conf, "Query3b");
            orderJob.setJarByClass(Query3b.class);

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
       // FileSystem.get(new Configuration()).delete(partitionFile, false);
        FileSystem.get(new Configuration()).delete(outputStage, true);

        /* Wait for job termination */
        System.exit(code);

    }
}
