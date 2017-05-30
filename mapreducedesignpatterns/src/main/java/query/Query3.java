package query;

import com.google.gson.Gson;
import designpattern.ordering.TotalOrdering;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import sun.security.krb5.internal.ccache.CCacheOutputStream;
import util.FilmRating;
import util.Films;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Calendar;
import java.util.regex.Pattern;

/**
 * Created by alessandro on 26/05/2017.
 */
public class Query3 {

    public static class PreOldMapper extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable outkey = new IntWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            outkey.set(Integer.valueOf(key.toString()));
            context.write(outkey, value);
            //la chiave non è ancora ribaltata 50=50,49=49 ...
            context.getCounter("SINGLE_COUNT", "" + (50 - Integer.valueOf(key.toString()))).increment(1);
        }

    }

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
            String content = new String();
            String line = value.toString();
            String[] parts = line.split(",");

            if (valuePrefix.equals("R")) {
                Calendar bottomLimit = Calendar.getInstance();
                bottomLimit.set(2013, Calendar.APRIL, 1);
                Calendar temp1 = Calendar.getInstance();
                temp1.setTimeInMillis(Long.parseLong(parts[3]) * 1000);
                Calendar middleLimit = Calendar.getInstance();
                middleLimit.set(2014, Calendar.APRIL, 1);

                if (temp1.getTime().compareTo(bottomLimit.getTime()) < 0)
                    return;
                else if (temp1.getTime().compareTo(middleLimit.getTime()) >= 0) {
                    content = "L" + parts[2];
                } else {

                    content = "P" + parts[2];
                }
                movieId = parts[1];


            } else {
                movieId = parts[0];
                for (int j = 1; j < parts.length - 1; j++)
                    content += parts[j];//title
            }
            outKey.set(Integer.parseInt(movieId));
            outValue.set(valuePrefix + content);
            context.write(outKey, outValue);

        }
    }

    public static class DatePartitioner extends Partitioner<IntWritable, Text> {

        public int getPartition(IntWritable key, Text value, int numPartitions) {
            // System.out.println("PARTITIONER "+ key.get()+":"+(key.get()) % numPartitions);
            return (50 - key.get());
            /*la posizione è inversamente proporzionale alle stelle guadagnate
             5.0 stelle = 50 -> partition[0]
             4.9 stelle = 49 -> partition[1]
             ...
              */
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

        public enum ValueType {RATING, FILM, UNKNOWN, PREV, LAST}
        //private Gson gson = new Gson();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            FilmRating films = new FilmRating();
            for (Text t : values) {

                String value = t.toString();
                if (ValueType.FILM.equals(discriminate(value))) {
                    films.setTitle(key + ":" + getContent(value));
                } else if (ValueType.RATING.equals(discriminate(value))) {
                    if (ValueType.PREV.equals(discriminateRating(value)))
                        films.addRatingPrev(Double.parseDouble(getRatingContent(value)));
                    else films.addRatingLast(Double.parseDouble(getRatingContent(value)));

                }

            }
            Double d = Math.floor(films.getRatingAvgPrev() * 10);
            //System.out.print("KEY"+d.toString().split(Pattern.quote("."))[0]);
            String[] array = d.toString().split(Pattern.quote("."));
            if (films.getRatingNumberPrev() > 5.0)
                context.write(new Text(array[0]), new Text(films.getTitle() + ":" + films.getRatingAvgLast()));
            else
                context.write(new Text("0"), new Text(films.getTitle() + ":" + films.getRatingAvgLast()));
        }


        private ValueType discriminate(String value) {

            char d = value.charAt(0);
            switch (d) {
                case 'R':
                    return ValueType.RATING;
                case 'F':
                    return ValueType.FILM;
            }

            return ValueType.UNKNOWN;
        }

        private String getContent(String value) {
            return value.substring(1);
        }

        private String getRatingContent(String value) {
            return value.substring(2);
        }

        private ValueType discriminateRating(String value) {

            char d = value.charAt(1);
            switch (d) {
                case 'L':
                    return ValueType.LAST;
                case 'P':
                    return ValueType.PREV;
            }

            return ValueType.UNKNOWN;
        }
    }


    public static class OrderingPhaseReducer extends Reducer<IntWritable, Text, Text, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // System.out.println("KEY REDUCER"+key.toString());
            long count = 0;
            /*int keyI= 50-key.get();
            *//*i valori con chiave j controllano il #tuple con chiave k>j e
            / quindi quelle con valutazione maggiore di quella di j
            *//*
            for(int i=0;i<keyI;i++) {
                count += context.getCounter("SINGLE_COUNT", "" + i).getValue();
                System.out.println("reduce["+key.get()+"]:"+count+"adding("+context.getCounter("SINGLE_COUNT", "" + i).getValue()+")");
            }*/
            for (Text t : values) {
                //context.write(new Text(key.toString()),t);
                count++;
                context.write(new Text(key.toString()), new Text(t + ":" + count));
            }
        }


    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query3");
        job.setJarByClass(Query3.class);
       // Path partitionFile = new Path(args[2] + "_partitions.lst");
        Path unionStage = new Path(args[2] + "_union");
        //Path outputStage = new Path(args[2] + "_staging");
        Path preOldPositioning = new Path(args[2] + "_preOldPositioning");
        Path oldPositioning = new Path(args[2]);


        job.setMapOutputKeyClass(IntWritable.class);
        /* Reduce function */
        job.setReducerClass(TopicHierarchyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(30);

        /* Map function, from multiple input file
         * arg[0] rating
         * arg[1] film*/
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FilmMapper.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, unionStage);


        int code = job.waitForCompletion(true) ? 0 : 1;

        /*FileUtil.copyMerge(FileSystem.get(new Configuration()),unionStage,
                FileSystem.get(new Configuration()),new Path(unionStage+""),true,conf,"");
*/
        if (code == 0) {
            Job orderJob = Job.getInstance(conf, "Query3");
           /* cs.addGroup("SINGLE_COUNT","SINGLE_COUNT");
            cs.addGroup("CUMULATIVE_COUNT","CUMULATIVE_COUNT");
            for(int i=0;i<50;i++) {
                cs.getGroup("SINGLE_COUNT").addCounter("" + i, "" + i, 0);
                cs.getGroup("CUMULATIVE_COUNT").addCounter("" + i, "" + i, 0);
            }*/
            orderJob.setJarByClass(Query3.class);

        /* Map: samples data; Reduce: identity function */
            orderJob.setMapperClass(PreOldMapper.class);
            orderJob.setMapOutputKeyClass(IntWritable.class);
            orderJob.setMapOutputValueClass(Text.class);
            orderJob.setReducerClass(OrderingPhaseReducer.class);
            orderJob.setNumReduceTasks(51);
            orderJob.setOutputKeyClass(Text.class);
            orderJob.setOutputValueClass(Text.class);

            orderJob.setPartitionerClass(DatePartitioner.class);

        /* Set input and output files */
            orderJob.getConfiguration().set("mapred.textoutputformat.separator", "");

            orderJob.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(orderJob, unionStage);
            TextOutputFormat.setOutputPath(orderJob, preOldPositioning);
         /*   orderJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputPath(orderJob, preOldPositioning);*/

        /* Submit the job and get completion code. */
            code = orderJob.waitForCompletion(true) ? 0 : 2;


            Counters cs = orderJob.getCounters();
            for (int i = 1; i < 50; i++) {
                Counter c = cs.findCounter("SINGLE_COUNT", "" + i);
                System.out.println("count[" + i + "]:" + c.getDisplayName() + ":" + c.getName() + ":" + c.getValue());
            }

            for (int i = 1; i < 50; i++) {
                Counter c = cs.findCounter("SINGLE_COUNT", "" + i);
                c.increment(cs.findCounter("SINGLE_COUNT", "" + (i - 1)).getValue());

                System.out.println("count[" + i + "]:" + c.getDisplayName() + ":" + c.getName() + ":" + c.getValue());
            }
            /**terzo step*/
           /* if (code == 0) {
                Job positioningJob = Job.getInstance(conf, "Query3");
           *//* cs.addGroup("SINGLE_COUNT","SINGLE_COUNT");
            cs.addGroup("CUMULATIVE_COUNT","CUMULATIVE_COUNT");
            for(int i=0;i<50;i++) {
                cs.getGroup("SINGLE_COUNT").addCounter("" + i, "" + i, 0);
                cs.getGroup("CUMULATIVE_COUNT").addCounter("" + i, "" + i, 0);
            }*//*
                positioningJob.setJarByClass(Query3.class);

        *//* Map: samples data; Reduce: identity function *//*
                positioningJob.setMapperClass(PreOldMapper.class);
                positioningJob.setMapOutputKeyClass(IntWritable.class);
                positioningJob.setMapOutputValueClass(Text.class);
                positioningJob.setReducerClass(OrderingPhaseReducer.class);
                positioningJob.setNumReduceTasks(51);
                positioningJob.setOutputKeyClass(Text.class);
                positioningJob.setOutputValueClass(Text.class);

                positioningJob.setPartitionerClass(DatePartitioner.class);

        *//* Set input and output files *//*
                positioningJob.getConfiguration().set("mapred.textoutputformat.separator", "");

                positioningJob.setInputFormatClass(SequenceFileInputFormat.class);
                SequenceFileInputFormat.setInputPaths(positioningJob, preOldPositioning);
                TextOutputFormat.setOutputPath(orderJob, oldPositioning);
         *//*   orderJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputPath(orderJob, preOldPositioning);*//*


        *//* Submit the job and get completion code. *//*
                code = orderJob.waitForCompletion(true) ? 0 : 3;


            }*/

        /* Clean up the partition file and the staging directory */
           // FileSystem.get(new Configuration()).delete(partitionFile, false);
            //FileSystem.get(new Configuration()).delete(outputStage, true);
            FileSystem.get(new Configuration()).delete(new Path(unionStage + "/*"), true);
            FileSystem.get(new Configuration()).delete(unionStage, true);

        /* Wait for job termination */
            System.exit(code);
        }
    }
}
