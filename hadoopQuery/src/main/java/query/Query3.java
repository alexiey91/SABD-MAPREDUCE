package query;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import util.FilmRating;
import java.io.IOException;
import java.util.Calendar;
import java.util.regex.Pattern;

import static java.lang.StrictMath.max;

/**
 * Created by alessandro on 26/05/2017.
 */
public class Query3 {

    public static class FinalOrderingMapper extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            //il contatore della chiave k = contatore[50-k], k compr [0,50]
            if(Integer.valueOf(key.toString())<=10)
                context.write(new IntWritable(Integer.valueOf(key.toString())), value);
        }

    }

    public static class LastMapper extends Mapper<Object, Text, IntWritable, Text>{
        private IntWritable outkey = new IntWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            //value è del tipo id:titolo:vecchia posizione:posizione locale ultimo anno risp all'id
            //pos corrisponde al numero totale di film con rating superiore a key
            long pos = max(0,context.getConfiguration().getLong(""+(49-Integer.valueOf(key.toString())),0));

            String[] ar= value.toString().split(":");
            pos += Long.parseLong(ar[ar.length-1]);
           // outkey.set((int)pos);
            outkey.set(Integer.valueOf(key.toString()));


            String outValue= ar[0];
            for(int k=1;k<ar.length-1;k++)
                outValue+=":"+ar[k];
            //new value è del tipo id:titolo:vecchia posizione:posizione iniziale fascia rating corrente
            context.write(outkey,new Text(outValue+":Position:"+pos));
        }
    }
    public static class PostOldMapper extends Mapper<Object, Text, IntWritable, Text>{
        private IntWritable outkey = new IntWritable();

        public void map(Object key, Text value, Context context)
                        throws IOException, InterruptedException {
            //value è del tipo id:titolo:newRate:posizione risp all'id
            //pos corrisponde al numero totale di film con rating superiore a key
            long pos = max(0,context.getConfiguration().getLong(""+(49-Integer.valueOf(key.toString())),0));

            //outkey.set(Integer.valueOf(key.toString()));
            String[] ar= value.toString().split(":");
            pos += Long.parseLong(ar[ar.length-1]);
            Double newRate = Math.floor(Double.parseDouble(ar[ar.length-2])*10);
            String nRt= newRate.toString().split(Pattern.quote("."))[0];
            outkey.set(Integer.parseInt(nRt));
            String outValue= ar[0];
            for(int k=1;k<ar.length-2;k++)
                outValue+=":"+ar[k];
            outValue+=" --oldPosition:"+pos;
            context.getCounter("SINGLE_COUNT", "" + (50 - Integer.parseInt(nRt))).increment(1);

            context.write(outkey,new Text(outValue));


            //System.out.println(key.toString()+"+++-----------------POS----------------:"+pos);

            //context.write(outkey,new Text(value.toString()+"-oldPosition:"+pos));
        }
    }

    public static class PreOldMapper extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable outkey = new IntWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            //il contatore della chiave k = contatore[50-k], k compr [0,50]
            context.getCounter("SINGLE_COUNT", "" + (50 - Integer.valueOf(key.toString()))).increment(1);

            outkey.set(Integer.valueOf(key.toString()));
            context.write(outkey, value);
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
                    content += ":"+parts[j];//title
            }
            outKey.set(Integer.parseInt(movieId));
            outValue.set(valuePrefix + content);
            context.write(outKey, outValue);

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

    public static class UnionReducer extends
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
            if (films.getRatingNumberLast() > 5.0) {
                if (films.getRatingNumberPrev() > 5.0)
                    context.write(new Text(array[0]), new Text(films.getTitle() + ":" + films.getRatingAvgLast()));
                else
                    context.write(new Text("0"), new Text(films.getTitle() + ":" + films.getRatingAvgLast()));
            }
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

            for (Text t : values) {
                //context.write(new Text(key.toString()),t);
                count++;
                context.write(new Text(key.toString()), new Text(t + ":" + count));
            }
        }
    }
    public static class PostOrderingReducer extends Reducer<IntWritable, Text, Text, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // System.out.println("KEY REDUCER"+key.toString());
            long count = 0;

            for (Text t : values) {
                count++;
                //context.write(new Text(key.toString()),t);
                context.write(new Text(key.toString()), new Text(t.toString() + ":" + count));
            }
        }
    }
    public static class LastPositionReducer extends Reducer<IntWritable, Text, Text, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int pos;

            for (Text t : values) {
                String[] ar= t.toString().split(":");
                pos = Integer.parseInt(ar[ar.length - 1]);
                String outValue= ar[0];
                for(int k=1;k<ar.length-2;k++)
                    outValue+=":"+ar[k];

                //context.write(new Text(key.toString()),t);
                context.write(new Text(pos+""),new Text(outValue));
                //context.write(new Text(outkey+""),t);
            }
        }
    }
    public static class FinalOrderingReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text t : values) {
                context.write(new Text(key.toString()+":::"+t.toString()), NullWritable.get());
            }

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
    public static class FinalPartitioner extends Partitioner<IntWritable, Text> {

        public int getPartition(IntWritable key, Text value, int numPartitions) {
            // System.out.println("PARTITIONER "+ key.get()+":"+(key.get()) % numPartitions);
            return (key.get()-1)%numPartitions;

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query3");
        job.setJarByClass(Query3.class);
        Path unionStage = new Path(args[2] + "_union");
        Path preOldPositioning = new Path(args[2] + "_preOldPositioning");
        Path oldPositioning = new Path(args[2]+"_oldPositioning");
        Path finalPositioning= new Path(args[2]+"_newPos");
        Path finalResult = new Path(args[2]);


        job.setMapOutputKeyClass(IntWritable.class);
        /* Reduce function */
        job.setReducerClass(UnionReducer.class);
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

            orderJob.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(orderJob, unionStage);

            orderJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputPath(orderJob, preOldPositioning);
//             TextOutputFormat.setOutputPath(orderJob, preOldPositioning);
         /*   orderJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputPath(orderJob, preOldPositioning);*/

        /* Submit the job and get completion code. */
            code = orderJob.waitForCompletion(true) ? 0 : 2;


            Counters cs = orderJob.getCounters();
            for (int i = 0; i < 51; i++) {
                Counter c = cs.findCounter("SINGLE_COUNT", "" + i);
                //System.out.println("count[" + i + "]:" + c.getDisplayName() + ":" + c.getName() + ":" + c.getValue());
            }
            Job positioningJob = Job.getInstance(conf, "Query3");

            positioningJob.getConfiguration().setLong("0", cs.findCounter("SINGLE_COUNT", "0").getValue());
            for (int i = 1; i < 51; i++) {

                Counter c = cs.findCounter("SINGLE_COUNT", "" + i);
                c.increment(cs.findCounter("SINGLE_COUNT", "" + (i - 1)).getValue());
                positioningJob.getConfiguration().setLong(c.getName(), c.getValue());

                //System.out.println("count[" + i + "]:" + c.getDisplayName() + ":" + c.getName() + ":" + c.getValue());
            }
                /**terzo step*/
            if (code == 0) {

                positioningJob.setJarByClass(Query3.class);

                /* Map: samples data; Reduce: identity function */
                positioningJob.setMapperClass(PostOldMapper.class);
                positioningJob.setMapOutputKeyClass(IntWritable.class);
                positioningJob.setMapOutputValueClass(Text.class);
                positioningJob.setReducerClass(PostOrderingReducer.class);
                positioningJob.setNumReduceTasks(51);
                positioningJob.setOutputKeyClass(Text.class);
                positioningJob.setOutputValueClass(Text.class);

                positioningJob.setPartitionerClass(DatePartitioner.class);

                /* Set input and output files */
                //positioningJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");

                positioningJob.setInputFormatClass(SequenceFileInputFormat.class);
                SequenceFileInputFormat.setInputPaths(positioningJob, preOldPositioning);
                //TextOutputFormat.setOutputPath(positioningJob, oldPositioning);
                positioningJob.setOutputFormatClass(SequenceFileOutputFormat.class);
                SequenceFileOutputFormat.setOutputPath(positioningJob, oldPositioning);

                code = positioningJob.waitForCompletion(true) ? 0 : 3;

                Job lastJob = Job.getInstance(conf, "Query3");
                cs = positioningJob.getCounters();
                lastJob.getConfiguration().setLong("0", cs.findCounter("SINGLE_COUNT", "0").getValue());
                for (int i = 1; i < 51; i++) {

                    Counter c = cs.findCounter("SINGLE_COUNT", "" + i);
             //       System.out.println("count[" + i + "]:" + c.getDisplayName() + ":" + c.getName() + ":" + c.getValue());

                    c.increment(cs.findCounter("SINGLE_COUNT", "" + (i - 1)).getValue());
                    lastJob.getConfiguration().setLong(c.getName(), c.getValue());

             //       System.out.println("*count[" + i + "]:" + c.getDisplayName() + ":" + c.getName() + ":" + c.getValue());
                }

                /**quarto step*/
                if (code == 0) {

                    lastJob.setJarByClass(Query3.class);

                    /* Map: samples data; Reduce: identity function */
                    lastJob.setMapperClass(LastMapper.class);
                    lastJob.setMapOutputKeyClass(IntWritable.class);
                    lastJob.setMapOutputValueClass(Text.class);
                    lastJob.setReducerClass(LastPositionReducer.class);
                    lastJob.setNumReduceTasks(50);
                    lastJob.setOutputKeyClass(Text.class);
                    lastJob.setOutputValueClass(Text.class);

                    lastJob.setPartitionerClass(DatePartitioner.class);


                    lastJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");

                    lastJob.setInputFormatClass(SequenceFileInputFormat.class);
                    SequenceFileInputFormat.setInputPaths(lastJob, oldPositioning);
                    //TextOutputFormat.setOutputPath(lastJob, finalPositioning);
                    lastJob.setOutputFormatClass(SequenceFileOutputFormat.class);
                    SequenceFileOutputFormat.setOutputPath(lastJob, finalPositioning);

                    /* Submit the job and get completion code. */
                    code = lastJob.waitForCompletion(true) ? 0 : 4;
                    /**quinto step*/
                    if (code == 0) {
                        Job orderFinalJob = Job.getInstance(conf, "Query3");

                        orderFinalJob.setJarByClass(Query3.class);

                    /* Map: samples data; Reduce: identity function */
                        orderFinalJob.setMapperClass(FinalOrderingMapper.class);
                        orderFinalJob.setReducerClass(FinalOrderingReducer.class);
                        orderFinalJob.setMapOutputKeyClass(IntWritable.class);
                        orderFinalJob.setMapOutputValueClass(Text.class);
                        orderFinalJob.setNumReduceTasks(1);
                        orderFinalJob.setOutputKeyClass(Text.class);
                        orderFinalJob.setOutputValueClass(Text.class);

                        orderFinalJob.setPartitionerClass(FinalPartitioner.class);


                        orderFinalJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");

                        orderFinalJob.setInputFormatClass(SequenceFileInputFormat.class);
                        SequenceFileInputFormat.setInputPaths(orderFinalJob, finalPositioning);
                        TextOutputFormat.setOutputPath(orderFinalJob, finalResult);

                    /* Submit the job and get completion code. */
                        code = orderFinalJob.waitForCompletion(true) ? 0 : 5;

                    }
                }
            }


        }
        /* Clean up the partition file and the staging directory */

        FileSystem.get(new Configuration()).delete(new Path(unionStage + "/*"), true);
        FileSystem.get(new Configuration()).delete(unionStage, true);
        FileSystem.get(new Configuration()).delete(new Path( preOldPositioning+ "/*"), true);
        FileSystem.get(new Configuration()).delete(preOldPositioning, true);
        FileSystem.get(new Configuration()).delete(new Path(oldPositioning + "/*"), true);
        FileSystem.get(new Configuration()).delete(oldPositioning, true);


        System.exit(code);
    }
}
