package query;

import com.google.gson.Gson;
import designpattern.partitioning.PartitionDataByYear;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import util.Films;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

public class Query1 {

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
                Calendar temp = Calendar.getInstance();
                temp.set(2000,Calendar.JANUARY,1);
                Calendar temp1 = Calendar.getInstance();
                temp1.setTimeInMillis(Long.parseLong(parts[3])*1000);
                /*if(temp1.getTime().compareTo(temp.getTime())<0)
                    System.out.println("VECCHIO RATING"+temp1.getTime());*/
                if(Double.parseDouble(parts[2])< 4.0 || temp1.getTime().compareTo(temp.getTime())<0)
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

    public static class TopicHierarchyReducer extends
            //Reducer<IntWritable, Text, Text, NullWritable> {
            Reducer<IntWritable, Text, Text, Text> {

        public enum ValueType { RATING, FILM , UNKNOWN}
        private Gson gson = new Gson();

        @Override
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

            /* Serialize topic */
            String serializedTopic = gson.toJson(films);
            //System.out.println("FILMS-reducer"+serializedTopic+"Title"+films.getTitle());
            if(films.getRatingNumber() > (Double) 0.0)
                context.write(new Text(films.getTitle()), new Text(films.getRating().toString()));
                //context.write(new Text(serializedTopic), NullWritable.get());

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

        /* Create and configure a new MapReduce Job */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query1");
        job.setJarByClass(Query1.class);


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
        job.setNumReduceTasks(30);


        job.setPartitionerClass(Query1.DatePartitioner.class);

        /* Set output files/directories using command line arguments */
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setOutputFormatClass(TextOutputFormat.class);

        /* Wait for job termination */
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}