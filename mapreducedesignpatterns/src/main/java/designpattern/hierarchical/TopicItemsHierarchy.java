package designpattern.hierarchical;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import util.Topic;

import java.io.IOException;

public class TopicItemsHierarchy {

    public static abstract class GenericHierarchyMapper extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();
        private final String valuePrefix;

        protected GenericHierarchyMapper(String valuePrefix) {
            this.valuePrefix = valuePrefix;
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] parts = line.split("::");

            if (parts.length != 2)
                return;

            String id = parts[0];
            String content = parts[1];

            outKey.set(id);
            outValue.set(valuePrefix + content);
            context.write(outKey, outValue);

        }
    }

    public static class TopicMapper extends GenericHierarchyMapper {
        public TopicMapper() {
            super("T");
        }
    }

    public static class ItemMapper extends GenericHierarchyMapper {
        public ItemMapper() {
            super("I");
        }
    }

    public static class TopicHierarchyReducer extends
            Reducer<Text, Text, Text, NullWritable> {

        public enum ValueType { TOPIC, ITEM, UNKNOWN}
        private Gson gson = new Gson();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Topic topic = new Topic();
            for (Text t : values) {

                String value = t.toString();
                if (ValueType.TOPIC.equals(discriminate(value))){
                    topic.setTopic(getContent(value));
                } else if (ValueType.ITEM.equals(discriminate(value))){
                    topic.addItem(getContent(value));
                }

            }

            /* Serialize topic */
            String serializedTopic = gson.toJson(topic);
            context.write(new Text(serializedTopic), NullWritable.get());

        }

        private ValueType discriminate(String value){

            char d = value.charAt(0);
            switch (d){
                case 'T':
                    return ValueType.TOPIC;
                case 'I':
                    return ValueType.ITEM;
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
        Job job = Job.getInstance(conf, "TopicHierarchy");
        job.setJarByClass(TopicItemsHierarchy.class);


        /* Map function, from multiple input file */
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TopicMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ItemMapper.class);

        /* Reduce function */
        job.setReducerClass(TopicHierarchyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        /* Set output files/directories using command line arguments */
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setOutputFormatClass(TextOutputFormat.class);

        /* Wait for job termination */
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}