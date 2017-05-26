package query;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import util.FilmRating;
import util.Films;

import java.io.IOException;
import java.util.Calendar;

/**
 * Created by alessandro on 26/05/2017.
 */
public class Query3 {

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
                System.out.println("MovieId "+movieId);
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
    public static class RatingMapper extends Query3.GenericHierarchyMapper {
        public RatingMapper() {
            super("R");
        }
    }

    public static class FilmMapper extends Query3.GenericHierarchyMapper {
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
                if (Query3.TopicHierarchyReducer.ValueType.FILM.equals(discriminate(value))){
                    films.setTitle(key+":"+getContent(value));
                } else if (Query3.TopicHierarchyReducer.ValueType.RATING.equals(discriminate(value))){
                    if(ValueType.PREV.equals(discriminateRating(value)))
                    films.addRatingPrev(Double.parseDouble(getRatingContent(value)));
                    else films.addRatingLast(Double.parseDouble(getRatingContent(value)));

                }

            }

             context.write(new Text(films.getRatingAvgPrev().toString()),new Text(key+":"+films.getTitle()+":"+films.getRatingAvgLast()));

        }

        private Query3.TopicHierarchyReducer.ValueType discriminate(String value){

            char d = value.charAt(0);
            switch (d){
                case 'R':
                    return Query3.TopicHierarchyReducer.ValueType.RATING;
                case 'F':
                    return Query3.TopicHierarchyReducer.ValueType.FILM;
            }

            return Query3.TopicHierarchyReducer.ValueType.UNKNOWN;
        }

        private String getContent(String value){
            return value.substring(1);
        }

        private String getRatingContent(String value){
            return value.substring(2);
        }

        private Query3.TopicHierarchyReducer.ValueType discriminateRating(String value){

            char d = value.charAt(1);
            switch (d){
                case 'L':
                    return Query3.TopicHierarchyReducer.ValueType.LAST;
                case 'P':
                    return Query3.TopicHierarchyReducer.ValueType.PREV;
            }

            return Query3.TopicHierarchyReducer.ValueType.UNKNOWN;
        }


    }

    public static void main(String[] args) throws Exception {

        /* Create and configure a new MapReduce Job */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query3");
        job.setJarByClass(Query3.class);


        /* Map function, from multiple input file
         * arg[0] rating
         * arg[1] film*/
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Query3.RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Query3.FilmMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        /* Reduce function */
        job.setReducerClass(Query3.TopicHierarchyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(30);


        job.setPartitionerClass(Query3.DatePartitioner.class);

        /* Set output files/directories using command line arguments */
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setOutputFormatClass(TextOutputFormat.class);

        /* Wait for job termination */
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
