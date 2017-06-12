package query;

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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import util.Categories;

import java.io.IOException;
import java.util.ArrayList;

/**
 * La richiesta della query2 consiste nel calcolare i valori di media e varianza per ogni genere di film
 * presente nel dataset.
 * **/
public class Query2Hdfs {

    public static abstract class GenericHierarchyMapper extends Mapper<Object, Text, IntWritable, Text> {

        /**
         *  Il primo mapper ha il compito di prelevare ed etichettare i film ed i rating.
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
            String content = new String();
            String line = value.toString();
            String[] parts = line.split(",");

            if (valuePrefix.equals("R")) {
                movieId = parts[1];
                content = parts[2];//rating
            } else {
                movieId = parts[0];
                content = parts[parts.length - 1];//title
            }
            outKey.set(Integer.parseInt(movieId));
            outValue.set(valuePrefix + content);
            context.write(outKey, outValue);

        }
    }


    public static class SecondMapper extends Mapper<Object, Text, Text, Text> {
        /**Il secondo mapper propaga in avanti le tuple**/
        private Text outKey = new Text();
        private Text outValue = new Text();


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            outKey.set(key.toString());
            outValue.set(line);
            context.write(outKey, outValue);

        }
    }

    public static class DatePartitioner extends Partitioner<IntWritable, Text> {

        public int getPartition(IntWritable key, Text value, int numPartitions) {

            return (key.get()) % numPartitions;
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

        public enum ValueType {RATING, FILM, UNKNOWN}

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            ArrayList<Categories> categories = new ArrayList<Categories>();
            Categories blob = new Categories();
            for (Text t : values) {
                String value = t.toString();
                /** 1a */
                if (ValueType.FILM.equals(discriminate(value))) {
                    String[] cat = getContent(value).split("\\|");
                    for (String j : cat)//aggiunge alla lista le categorie prelevate dal film
                        categories = Categories.addExclusive(categories, j, 0.0, 0.0, 0.0);
                }/** 1b */ else if (ValueType.RATING.equals(discriminate(value))) {
                    blob.addRating(Double.parseDouble(getContent(value)));
                }
            }

            /** 2 */
            for (Categories c : categories)
                categories = Categories.addExclusive(categories, c.getGenres(), blob.getRatingNumber(),
                        blob.getRating(), blob.getRatingVar());

            for (Categories c : categories) {
                if (c.getRatingVar().isNaN() || c.getRatingVar() == 0.0)
                    context.write(new Text(c.getGenres()), new Text(c.getRatingNumber().toString() + ":" +
                            c.getRating().toString() + ":0.0"));
                else
                    context.write(new Text(c.getGenres()), new Text(c.getRatingNumber().toString() + ":" +
                            c.getRating().toString() + ":" + c.getRatingVar().toString()));
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

    }

    public static class SecondReducer extends
            Reducer<Text, Text, Text, NullWritable> {
        /** Il secondo reducer ha il compito di calcolare la media dei valori di media e varianza
         *  presi dallo step precedente**/

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Categories categories = new Categories();
            categories.setGenres(key.toString());

            for (Text t : values) {
                String[] s = t.toString().split(":");
                categories.addRatingMod(Double.parseDouble(s[0]), Double.parseDouble(s[1]), Double.parseDouble(s[2]));
            }

            context.write(new Text(categories.getGenres() + ":[AVG:"
                    + categories.getRating() + ",VAR:" + categories.getRatingVar() + "]"), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {

         /**Job #1:Analyze phase**/
        Configuration conf = new Configuration();
        //Path partitionFile = new Path(args[2] + "_partitions.lst");
        Path outputStage = new Path(args[2] + "_staging");
        Path outputOrder = new Path(args[2]);

        Job job = Job.getInstance(conf, "Query2Hdfs");
        job.setJarByClass(Query2Hdfs.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setReducerClass(TopWaterfallReducer.class);
        job.setNumReduceTasks(30);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(Query2Hdfs.DatePartitioner.class);

       /* Set input and output files
        Map function, from multiple input file
                * arg[0] rating
                * arg[1] film*/
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FilmMapper.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, outputStage);

        long startJob = System.currentTimeMillis();

        int code = job.waitForCompletion(true) ? 0 : 1;

        long finishJob =System.currentTimeMillis()-startJob;
        System.out.println("Tempo di esecuzione Query2 (HDFS) 1°MapReduce: "+finishJob+" ms");

        if (code == 0) {

            /**Job #2 **/
            Job orderJob = Job.getInstance(conf, "Query2Hdfs");
            orderJob.setJarByClass(Query2Hdfs.class);
            orderJob.setMapperClass(SecondMapper.class);
            orderJob.setReducerClass(SecondReducer.class);
            orderJob.setNumReduceTasks(15);

            orderJob.setOutputKeyClass(Text.class);
            orderJob.setOutputValueClass(Text.class);

            orderJob.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(orderJob, outputStage);
            TextOutputFormat.setOutputPath(orderJob, outputOrder);

            long StartQuery2 = System.currentTimeMillis();
            code = orderJob.waitForCompletion(true) ? 0 : 2;
            long FinishQuery2= System.currentTimeMillis()-StartQuery2;
            System.out.println("Tempo esecuzione Query2 (HDFS) 2°Map Reduce= "+FinishQuery2+" ms");

            System.out.println("Tempo totale esecuzione Query2 (HDFS): "+(finishJob +FinishQuery2)+" ms");

        }

        //FileSystem.get(new Configuration()).delete(partitionFile, false);
        FileSystem.get(new Configuration()).delete(outputStage, true);
        Path[] fragments = new Path[15];
        for (int i = 0; i < 10; i++) {
            fragments[i] = new Path(outputOrder + "part-r-0000" + i);
            if (i >= 10)
                fragments[i] = new Path(outputOrder + "part-r-000" + i);
        }
        FileUtil.copyMerge(FileSystem.get(new Configuration()), outputOrder,
                FileSystem.get(new Configuration()), new Path(outputOrder + "Compact"), true, conf, "");

        System.exit(code);
    }
}


