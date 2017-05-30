package query;

import com.google.gson.Gson;
import designpattern.ordering.TotalOrdering;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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
import util.Categories;
import util.Films;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;

public class Query2 {

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
                movieId = parts[1];
                content = parts[2];//rating
            }else{
                movieId = parts[0];
                content = parts[parts.length-1];//title
            }
            outKey.set(Integer.parseInt(movieId));
            outValue.set(valuePrefix + content);
            context.write(outKey, outValue);

        }
    }
    /*public static abstract class WaterfallMapper extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();
        private final String valuePrefix;

        protected WaterfallMapper(String valuePrefix) {
            this.valuePrefix = valuePrefix;
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String movieId;
            String content= new String();
            String line = value.toString();


            outKey.set(key.toString());
            outValue.set(valuePrefix + content);
            context.write(outKey, outValue);

        }
    }*/
    public static class SecondMapper extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

           // System.out.println("2MAP:"+ key.toString());

            outKey.set(key.toString());
            outValue.set(line);
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


    public static class TopWaterfallReducer extends
            Reducer<IntWritable, Text, Text, Text> {

        public enum ValueType { RATING, FILM , UNKNOWN}
       // private Gson gson = new Gson();

        /*
        * 1a    Se il messaggio viene dai film è necessario prelevare le categorie per
        *       inserirle in categories(arrayList).
        * 1b    in Blob viene effettuata la media e la varianza di tutte le votazioni del film.
        * 2     per ogni elemento di cui la categoria è stata immessa nel punto 1a si
        *       si completano le info con i dati presi da 1b
        * */
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            ArrayList<Categories> categories = new ArrayList<Categories>();
            Categories blob = new Categories();//valori da aggiornare su ogni categoria
            for (Text t : values) {
                String value = t.toString();
                /* 1a */
                if(ValueType.FILM.equals(discriminate(value))){
                    String[] cat= getContent(value).split("\\|");
                    for(String j : cat)//prova ad aggiungere tutte le categorie prelevate dal film
                        categories= Categories.addExclusive(categories,j,0.0,0.0,0.0);
                }/* 1b */
                else if (ValueType.RATING.equals(discriminate(value))){
                    blob.addRating(Double.parseDouble(getContent(value)));
                }
            }
           // System.out.println("Reducer:#"+blob.getRatingNumber()+","+blob.getRating()+","+blob.getRatingVar());
            /* 2 */
            for(Categories c: categories)
                categories = Categories.addExclusive(categories,c.getGenres(),blob.getRatingNumber(),
                        blob.getRating(),blob.getRatingVar());

            /* Serialize topic */
            //String serializedTopic = gson.toJson(categories);
            for(Categories c :categories) {
                if(c.getRatingVar().isNaN()||c.getRatingVar()== 0.0)
                    context.write(new Text(c.getGenres()), new Text(c.getRatingNumber().toString() + ":" +
                        c.getRating().toString() + ":0.0"));
                else
                    context.write(new Text(c.getGenres()), new Text(c.getRatingNumber().toString() + ":" +
                        c.getRating().toString() + ":" + c.getRatingVar().toString()));
            }
            //System.out.println("Reducer:"+serializedTopic.toString());

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

    }
    public static class SecondReducer extends
            Reducer<Text, Text, Text, NullWritable> {

       // private Gson gson = new Gson();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Categories categories = new Categories();
            categories.setGenres(key.toString());

            for (Text t : values) {
                //n:avg:var
                String[] s = t.toString().split(":");
                //System.out.println("S-REDUCER:"+key.toString()+":"+s[0]+":"+s[1]+":"+s[2]);
                categories.addRatingMod(Double.parseDouble(s[0]),Double.parseDouble(s[1]),Double.parseDouble(s[2]));
                //bisogna fare una media pesata incrementale

            }
            //System.out.println("S-REDUCER:"+key.toString()+":"+categories.getRating()+":"+categories.getRatingVar());
            /* Serialize topic */
           // String serializedTopic = gson.toJson(categories);

            context.write(new Text(categories.getGenres()+":[AVG:"
           +categories.getRating()+",VAR:"+categories.getRatingVar()+"]" ),NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {

        /* **** Job #1: Analyze phase **** */

        /* Create and configure a new MapReduce Job */
        Configuration conf = new Configuration();
        Path partitionFile = new Path(args[2] + "_partitions.lst");
        Path outputStage = new Path(args[2] + "_staging");
        Path outputOrder = new Path(args[2]);

        Job job = Job.getInstance(conf, "Query2");
        job.setJarByClass(Query2.class);

        /* Reduce function */
        job.setReducerClass(TopWaterfallReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(30);

//        job.setMapOutputKeyClass(IntWritable.class);


        job.setPartitionerClass(Query2.DatePartitioner.class);

        /* Set input and output files */
        /* Map function, from multiple input file
         * arg[0] rating
         * arg[1] film*/
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FilmMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, outputStage);

        /* Submit the job and get completion code. */
        int code = job.waitForCompletion(true) ? 0 : 1;
        /* ********************************************************* */
        if (code == 0) {

            /* **** Job #2: Ordering phase **** */
            Job orderJob = Job.getInstance(conf, "Query2");
            orderJob.setJarByClass(Query2.class);


            /* Map: identity function outputs the key/value pairs in the SequenceFile */
            orderJob.setMapperClass(Query2.SecondMapper.class);

            /* Reduce: identity function (the important data is the key, value is null) */
            orderJob.setReducerClass(Query2.SecondReducer.class);
            orderJob.setNumReduceTasks(15);

            orderJob.setOutputKeyClass(Text.class);
            orderJob.setOutputValueClass(Text.class);


            /* Set input and output files: the input is the previous job's output */
            orderJob.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(orderJob, outputStage);

            TextOutputFormat.setOutputPath(orderJob, outputOrder);
            // Set the separator to an empty string

            /* Submit the job and get completion code. */
            code = orderJob.waitForCompletion(true) ? 0 : 2;
        }

        /* Clean up the partition file and the staging directory */
        FileSystem.get(new Configuration()).delete(partitionFile, false);
        FileSystem.get(new Configuration()).delete(outputStage, true);
        /*Path[] fragments = new Path[15];
        for(int i=0;i< 10;i++) {
            fragments[i] = new Path(outputOrder + "part-r-0000" + i);
            if (i>=10)
                fragments[i] = new Path(outputOrder + "part-r-000" + i);
        }*/
        FileUtil.copyMerge(FileSystem.get(new Configuration()),outputOrder,
                FileSystem.get(new Configuration()),new Path(outputOrder+"Compact"),true,conf,"");
        //FileSystem.get(new Configuration()).concat(new Path(outputOrder+"Compact"),fragments);

        /* Wait for job termination */
        System.exit(code);


    }
}