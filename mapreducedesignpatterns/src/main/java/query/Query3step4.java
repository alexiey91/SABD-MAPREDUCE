package query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import util.FilmRating;
import util.HBaseClient;

import java.io.IOException;
import java.util.Calendar;
import java.util.regex.Pattern;

import static java.lang.StrictMath.max;

/**
 *  La richiesta della query 3 consiste nel trovare i primi 10 film valutati nell'ultimo anno del dataset
 *  e valutarne la differenza rispetto alla posizione relativa all'anno precedente.
 *
 *  La differenza con la classe Query3step4Hdfs risiede nel salvare i dati su HBASE invece che su HDFS.
 */
public class Query3step4 {



    public static class LastMapper extends Mapper<Object, Text, IntWritable, Text>{
        private IntWritable outkey = new IntWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            //value è del tipo id:titolo:vecchia posizione:posizione locale ultimo anno risp all'id
            //pos corrisponde al numero totale di film con rating superiore a key
            long pos = max(0,context.getConfiguration().getLong(""+(49-Integer.valueOf(key.toString())),0));

            String[] ar= value.toString().split(":");
            pos += Long.parseLong(ar[ar.length-1]);
            outkey.set((int)pos);

            if(pos<=(long)10) {
                String outValue = ar[0];
                for (int k = 1; k < ar.length - 1; k++)
                    outValue += ":" + ar[k];
                //new value è del tipo id:titolo:vecchia posizione:posizione corrente
                context.write(outkey, new Text(outValue));
            }

        }
    }
    public static class PostOldMapper extends Mapper<Object, Text, IntWritable, Text>{
        private IntWritable outkey = new IntWritable();

        public void map(Object key, Text value, Context context)
                        throws IOException, InterruptedException {
            //value è del tipo id:titolo:newRate:posizione risp all'id
            //pos corrisponde al numero totale di film con rating superiore a key
            long pos = max(0,context.getConfiguration().getLong(""+(49-Integer.valueOf(key.toString())),0));

            String[] ar= value.toString().split(":");
            pos += Long.parseLong(ar[ar.length-1]);
            Double newRate = Math.floor(Double.parseDouble(ar[ar.length-2])*10);
            String nRt= newRate.toString().split(Pattern.quote("."))[0];
            outkey.set(Integer.parseInt(nRt));
            String outValue= ar[0];
            for(int k=1;k<ar.length-2;k++)
                outValue+=":"+ar[k];

            outValue+=":"+pos;
            context.getCounter("SINGLE_COUNT", "" + (50 - Integer.parseInt(nRt))).increment(1);

            context.write(outkey,new Text(outValue));

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

            String[] array = d.toString().split(Pattern.quote("."));
            if (films.getRatingNumberLast() > 15.0) {
                if (films.getRatingNumberPrev() > 15.0)
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

            long count = 0;

            for (Text t : values) {

                count++;
                context.write(new Text(key.toString()), new Text(t + ":" + count));
            }
        }
    }
    public static class PostOrderingReducer extends Reducer<IntWritable, Text, Text, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            long count = 0;

            for (Text t : values) {
                count++;

                context.write(new Text(key.toString()), new Text(t.toString() + ":" + count));
            }
        }
    }

    public static class FinalOrderingReducer extends TableReducer<IntWritable, Text, ImmutableBytesWritable> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text t : values) {
                String[] splt = t.toString().split(":");
                String title= splt[2];
                for(int j=3;j<splt.length-2;j++){
                    title+=":"+splt[j];
                }
                int delta=Integer.parseInt(splt[splt.length-1])-key.get();
                Put put = new Put(Bytes.toBytes(key.get()+""));
                put.addColumn(Bytes.toBytes("Title"), Bytes.toBytes("id"), Bytes.toBytes(splt[0]));
                put.addColumn(Bytes.toBytes("Title"), Bytes.toBytes("name"), Bytes.toBytes(title));
                //put.addColumn(Bytes.toBytes("Ranking"), Bytes.toBytes("actual"), Bytes.toBytes(key.toString()));
                put.addColumn(Bytes.toBytes("Ranking"), Bytes.toBytes("previous"), Bytes.toBytes(splt[splt.length-1]));
                put.addColumn(Bytes.toBytes("Ranking"), Bytes.toBytes("delta"), Bytes.toBytes(delta+""));
                context.write(null,put);

            }

        }

    }
    public static class DatePartitioner extends Partitioner<IntWritable, Text> {

        public int getPartition(IntWritable key, Text value, int numPartitions) {

            return (50 - key.get());
            /**la posizione è inversamente proporzionale alle stelle guadagnate
             *  5.0 stelle = 50 -> partition[0]
             *  4.9 stelle = 49 -> partition[1]
             *  ...
             **/
        }
    }
    public static class FinalPartitioner extends Partitioner<IntWritable, Text> {

        public int getPartition(IntWritable key, Text value, int numPartitions) {
            //System.out.println("PARTITIONER "+ key.get()+":"+(key.get()-1) % numPartitions);
            return (key.get()-1)%numPartitions;

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
       /* conf.set("fs.defaultFS", "hdfs://127.0.0.1:50070");
        conf.set("fs.defaultFS", "hdfs://master:54310");
        conf.set("mapreduce.jobtracker.address", "master:19888");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.address", "localhost:8088");
        */
        Job job = Job.getInstance(conf, "Query3step4");
        job.setJarByClass(Query3step4.class);
        Path unionStage = new Path(args[2] + "_union");
        Path preOldPositioning = new Path(args[2] + "_preOldPositioning");
        Path oldPositioning = new Path(args[2]+"_oldPositioning");

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
       /* HBaseClient client = new HBaseClient();
        if(!client.exists("query3"))
            client.createTable("query3","Title","Ranking");*/
        long startJob= System.currentTimeMillis();
        int code = job.waitForCompletion(true) ? 0 : 1;
        long finishJob =System.currentTimeMillis()-startJob;
        System.out.println("Tempo di esecuzione Query3 1°Map Reduce: "+finishJob+" ms");

        if (code == 0) {
            Job orderJob = Job.getInstance(conf, "Query3step4");

            orderJob.setJarByClass(Query3step4.class);

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


        /* Submit the job and get completion code. */
            long startJob2= System.currentTimeMillis();
            code = orderJob.waitForCompletion(true) ? 0 : 2;
            long finishJob2 =System.currentTimeMillis()-startJob2;
            System.out.println("Tempo di esecuzione Query3 2°MapReduce: "+finishJob2+" ms");


            Counters cs = orderJob.getCounters();
            for (int i = 0; i < 51; i++) {
                Counter c = cs.findCounter("SINGLE_COUNT", "" + i);

            }
            Job positioningJob = Job.getInstance(conf, "Query3step4");

            positioningJob.getConfiguration().setLong("0", cs.findCounter("SINGLE_COUNT", "0").getValue());
            for (int i = 1; i < 51; i++) {

                Counter c = cs.findCounter("SINGLE_COUNT", "" + i);
                c.increment(cs.findCounter("SINGLE_COUNT", "" + (i - 1)).getValue());
                positioningJob.getConfiguration().setLong(c.getName(), c.getValue());

            }
                /**terzo step*/
            if (code == 0) {

                positioningJob.setJarByClass(Query3step4.class);

                /* Map: samples data; Reduce: identity function */
                positioningJob.setMapperClass(PostOldMapper.class);
                positioningJob.setMapOutputKeyClass(IntWritable.class);
                positioningJob.setMapOutputValueClass(Text.class);
                positioningJob.setReducerClass(PostOrderingReducer.class);
                positioningJob.setNumReduceTasks(51);
                positioningJob.setOutputKeyClass(Text.class);
                positioningJob.setOutputValueClass(Text.class);

                positioningJob.setPartitionerClass(DatePartitioner.class);

                positioningJob.setInputFormatClass(SequenceFileInputFormat.class);
                SequenceFileInputFormat.setInputPaths(positioningJob, preOldPositioning);

                positioningJob.setOutputFormatClass(SequenceFileOutputFormat.class);
                SequenceFileOutputFormat.setOutputPath(positioningJob, oldPositioning);
                long startJob3 = System.currentTimeMillis();
                code = positioningJob.waitForCompletion(true) ? 0 : 3;

                long finishJob3 =System.currentTimeMillis()-startJob3;
                System.out.println("Tempo di esecuzione Query3 3°MapReduce: "+finishJob3+" ms");


                Job lastJob = Job.getInstance(conf, "Query3step4");
                cs = positioningJob.getCounters();
                lastJob.getConfiguration().setLong("0", cs.findCounter("SINGLE_COUNT", "0").getValue());
                for (int i = 1; i < 51; i++) {

                    Counter c = cs.findCounter("SINGLE_COUNT", "" + i);
                    c.increment(cs.findCounter("SINGLE_COUNT", "" + (i - 1)).getValue());
                    lastJob.getConfiguration().setLong(c.getName(), c.getValue());

                }

                /**quarto step*/
                if (code == 0) {

                    lastJob.setJarByClass(Query3step4.class);

                    /* Map: samples data; Reduce: identity function */
                    lastJob.setMapperClass(LastMapper.class);
                    lastJob.setMapOutputKeyClass(IntWritable.class);
                    lastJob.setMapOutputValueClass(Text.class);
                    lastJob.setNumReduceTasks(1);
                    lastJob.setPartitionerClass(FinalPartitioner.class);


                    lastJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");

                    lastJob.setInputFormatClass(SequenceFileInputFormat.class);
                    SequenceFileInputFormat.setInputPaths(lastJob, oldPositioning);

                    TableMapReduceUtil.initTableReducerJob(
                            "query3",        // output table
                            FinalOrderingReducer.class,    // reducer class
                            lastJob);
                    try {
                        long StartQuery3=System.currentTimeMillis();
                        code = lastJob.waitForCompletion(true) ? 0 : 4;
                        System.out.println("Counters"+ lastJob.getCounters().findCounter("Job Counters","Total time spent by all reduce tasks (ms)"));

                        long FinishQuery3= System.currentTimeMillis()-StartQuery3;
                        System.out.println("Tempo esecuzione Query3 4°MapReduce"+ FinishQuery3+" ms");

                    }catch (IOException e){
                        System.out.print("errore"+e);

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
