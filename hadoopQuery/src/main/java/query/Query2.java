package query;


import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import util.Categories;
import util.HBaseClient;

import java.io.IOException;
import java.util.ArrayList;


/**
 * La richiesta della query2 consiste nel calcolare i valori di media e varianza per ogni genere di film
 * presente nel dataset.
 **/

public class Query2 {
    /**
     *  Il primo mapper ha il compito di prelevare ed etichettare i film ed i rating.
     *  I rating avranno un header R ed i film F
     *  In uscita avremo <idFilm,F:Titolo> oppure <idFilm,R:Rating>
     *  **/

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



    public static class SecondMapper extends Mapper<Object, Text, IntWritable, Text> {
        /**Il secondo mapper propaga in avanti le tuple ma convertendo i generi in un codice univoco**/

        private IntWritable outKey = new IntWritable();
        private Text outValue = new Text();
        //private ImmutableBytesWritable outkey = new ImmutableBytesWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {



            String line = value.toString();

            switch(key.toString()) {
                case "Action" :
                    outKey.set(0);
                    break;
                case "Adventure" :
                    outKey.set(1);
                    break;
                case "Animation" :
                    outKey.set(2);
                    break;
                case "Children's" :
                    outKey.set(3);
                    break;
                case "Comedy" :
                    outKey.set(4);
                    break;
                case "Crime" :
                    outKey.set(5);
                    break;
                case "Documentary" :
                    outKey.set(6);
                    break;
                case "Drama" :
                    outKey.set(7);
                    break;
                case "Fantasy" :
                    outKey.set(8);
                    break;
                case "Film/-Noir" :
                    outKey.set(9);
                    break;
                case "Horror" :
                    outKey.set(10);
                    break;
                case "IMAX" :
                    outKey.set(11);
                    break;
                case "Musical" :
                    outKey.set(12);
                    break;
                case "Mystery" :
                    outKey.set(13);
                    break;
                case "Romance" :
                    outKey.set(14);
                    break;
                case "Sci/-Fi" :
                    outKey.set(15);
                    break;
                case "Thriller" :
                    outKey.set(16);
                    break;
                case "War" :
                    outKey.set(17);
                    break;
                case "Western" :
                    outKey.set(18);
                    break;
                case "(no genres listed)" :
                    outKey.set(19);
                    break;


            }

            outValue.set(key.toString()+":"+line);

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

        /** Il primo reducer deve associare ad i film le valutazioni e farne la media e la varianza.
         *  Una volta effettuate le stime deve creare tante tuple quante sono le categorie associate
         *  al film. Questo viene fatto nei seguenti step:
         * 1a    Se il messaggio viene dai film è necessario prelevare le categorie per
         *       inserirle nell'arrayList categories.
         * 1b    in Blob viene effettuata la media e la varianza di tutte le votazioni del film.
         * 2     si completano, con i dati presi nella fase 1b, tutte le categorie immesse nella fase 1a nella lista
         **/
        public enum ValueType { RATING, FILM , UNKNOWN}

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            ArrayList<Categories> categories = new ArrayList<Categories>();
            Categories blob = new Categories();
            for (Text t : values) {
                String value = t.toString();
                /* 1a */
                if(ValueType.FILM.equals(discriminate(value))){
                    String[] cat= getContent(value).split("\\|");
                    for(String j : cat)//aggiunge alla lista le categorie prelevate dal film
                        categories= Categories.addExclusive(categories,j,0.0,0.0,0.0);
                }/* 1b */
                else if (ValueType.RATING.equals(discriminate(value))){
                    blob.addRating(Double.parseDouble(getContent(value)));
                }
            }
            /* 2 */
            for(Categories c: categories)
                categories = Categories.addExclusive(categories,c.getGenres(),blob.getRatingNumber(),
                        blob.getRating(),blob.getRatingVar());

            for(Categories c :categories) {
                if(c.getRatingVar().isNaN()||c.getRatingVar()== 0.0)
                    context.write(new Text(c.getGenres()), new Text(c.getRatingNumber().toString() + ":" +
                        c.getRating().toString() + ":0.0"));
                else
                    context.write(new Text(c.getGenres()), new Text(c.getRatingNumber().toString() + ":" +
                        c.getRating().toString() + ":" + c.getRatingVar().toString()));
            }

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
            TableReducer<IntWritable, Text, ImmutableBytesWritable> {
        /** Il secondo reducer ha il compito di calcolare la media dei valori di media e varianza
         *  presi dallo step precedente**/


        public void reduce(IntWritable key, Iterable<Text> values,Context context)  throws IOException, InterruptedException {

            Categories categories = new Categories();


            for (Text t : values) {
                //n:avg:var
                String[] s = t.toString().split(":");
                categories.setGenres(s[0]);

                categories.addRatingMod(Double.parseDouble(s[1]),Double.parseDouble(s[2]),Double.parseDouble(s[3]));

            }

            Put put = new Put(Bytes.toBytes(categories.getGenres()));
            put.addColumn(Bytes.toBytes("Average"), Bytes.toBytes("count"), Bytes.toBytes(categories.getRating().toString()));
            put.addColumn(Bytes.toBytes("Variance"), Bytes.toBytes("count"), Bytes.toBytes(categories.getRatingVar().toString()));
            context.write(null,  put);

        }
    }




    public static void main(String[] args) throws Exception {


        /**Job #1:Analyze phase**/

        Configuration conf = HBaseConfiguration.create();
        /*conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
        conf.set("mapreduce.jobtracker.address", "alessandro-lenovo-g500:54311");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.address", "alessandro-lenovo-g500:8032");*/
        Path outputStage = new Path(args[2] + "_staging");

        Job job = Job.getInstance(conf, "Query2");
        job.setJarByClass(Query2.class);

        job.setReducerClass(TopWaterfallReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(30);
        job.setMapOutputKeyClass(IntWritable.class);

        job.setPartitionerClass(Query2.DatePartitioner.class);

        /* Set input and output files */
        /* Map function, from multiple input file
         * arg[0] rating
         * arg[1] film*/
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FilmMapper.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, outputStage);
        long startJob = System.currentTimeMillis();

        int code = job.waitForCompletion(true) ? 0 : 1;

        long finishJob =System.currentTimeMillis()-startJob;
        System.out.println("Tempo di esecuzione Query2 1°MapReduce: "+finishJob+" ms");

        if (code == 0) {

            /** Job #2 **/
            Job orderJob = Job.getInstance(conf, "Query2");
            orderJob.setJarByClass(Query2.class);


            /* Map: identity function outputs the key/value pairs in the SequenceFile */
            orderJob.setMapperClass(SecondMapper.class);
            orderJob.setMapOutputKeyClass(IntWritable.class);
            orderJob.setMapOutputValueClass(Text.class);
            /* Reduce: identity function (the important data is the key, value is null) */
            orderJob.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(orderJob, outputStage);

            /*HBaseClient client = new HBaseClient();
            if(!client.exists("query2"))
                client.createTable("query2","Average","Variance");
            */
            TableMapReduceUtil.initTableReducerJob(
                    "query2",        // output table
                    SecondReducer.class,    // reducer class
                    orderJob);
            orderJob.setNumReduceTasks(15);


            try {
                long StartQuery2 = System.currentTimeMillis();

                boolean b = orderJob.waitForCompletion(true);

                long FinishQuery2= System.currentTimeMillis()-StartQuery2;
                System.out.println("Tempo esecuzione Query2 2°Map Reduce= "+FinishQuery2+" ms");

                System.out.println("Tempo totale esecuzione Query2: "+(finishJob +FinishQuery2)+" ms");


            }catch (IOException e){
                System.out.print("errore"+e);

            }
        }

    }

}