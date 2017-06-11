package query;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import util.Films;

import java.io.IOException;
import java.util.Calendar;

public class Query1 {

    public static abstract class GenericHierarchyMapper extends Mapper<Object, Text, IntWritable, Text> {
        /**
         *  Il mapper ha il compito di prelevare ed etichettare i film ed i rating.
         *  I rating avranno un header R ed i film F
         *  In uscita avremo <idFilm,F:Titolo> oppure <idFilm,R:Rating>
         *  **/
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
                Calendar temp = Calendar.getInstance();
                temp.set(2000,Calendar.JANUARY,1);
                Calendar temp1 = Calendar.getInstance();
                temp1.setTimeInMillis(Long.parseLong(parts[3])*1000);
                if(temp1.getTime().compareTo(temp.getTime())<0)
                    return;
                movieId = parts[1];
                content = parts[2];//rating


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


    public static class TableReduce extends
            //Reducer<IntWritable, Text, Text, NullWritable> {
            TableReducer<IntWritable, Text, ImmutableBytesWritable> {
        /**Il reducer calcola la media di ogni film; se la media è <= 4.0 viene scartato
         * Salva i dati su Hbase**/

        public enum ValueType { RATING, FILM , UNKNOWN}

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Films films = new Films();
            for (Text t : values) {

                String value = t.toString();
                if (ValueType.FILM.equals(discriminate(value))){
                    films.setTitle(key+":"+getContent(value));
                } else if (ValueType.RATING.equals(discriminate(value))){
                    films.addRating(Double.parseDouble(getContent(value)));
                }

            }


            if(films.getRatingNumber() > (Double) 0.0 && films.getRating() > 4.0)
            {
                Put put = new Put(Bytes.toBytes(films.getTitle()));
                put.addColumn(Bytes.toBytes("RATING"), Bytes.toBytes("count"), Bytes.toBytes(films.getRating().toString()));

                put.addColumn(Bytes.toBytes("RATING_NUMBER"), Bytes.toBytes("count"), Bytes.toBytes(films.getRatingNumber().toString()));

                context.write(null, put);
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



    public static void main(String[] args) throws Exception {


        Configuration conf = HBaseConfiguration.create();
       /* conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
        conf.set("mapreduce.jobtracker.address", "alessandro-lenovo-g500:54311");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.address", "alessandro-lenovo-g500:8032");*/

        Job job = Job.getInstance(conf, "Query1");
        job.setJarByClass(Query1.class);


        /* Map function, from multiple input file
         * arg[0] rating
         * arg[1] film*/
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FilmMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        /*HBaseClient client = new HBaseClient();
        if(!client.exists("query1"))
            client.createTable("query1","RATING","RATING_NUMBER");
*/

        TableMapReduceUtil.initTableReducerJob(
                "query1",        // output table
                TableReduce.class,    // reducer class
                job,
                null);
        job.setNumReduceTasks(30);
        job.setPartitionerClass(DatePartitioner.class);

        try { long startJob = System.currentTimeMillis();

            boolean b = job.waitForCompletion(true);
            long finishJob =System.currentTimeMillis()-startJob;
            System.out.println("Tempo di esecuzione Query1: "+finishJob+" ms");


        }catch (IOException e){
            System.out.print("errore"+e);

        }
    }

}