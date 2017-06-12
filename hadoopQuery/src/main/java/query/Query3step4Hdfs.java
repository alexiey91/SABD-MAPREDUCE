package query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 *  La richiesta della query 3 consiste nel trovare i primi 10 film valutati nell'ultimo anno del dataset
 *  e valutarne la differenza rispetto alla posizione relativa all'anno precedente.
 **/
 public class Query3step4Hdfs {

    public static class LastMapper extends Mapper<Object, Text, IntWritable, Text>{

        /**
         * Il quarto mapper recupera i valori del contatore relativo al gruppo di film con
         * rating dell'ultimo anno maggiore di 1 rispetto al rating corrente:
         * se il film ha rating k controlla il valore di count_cumulative[k-1] ossia il minimo
         * numero di film che lo precedono.
         * Preso quel valore lo si somma alla posizione locale,
         * ossia quella all'interno del gruppo di film con la stessa votazione.
         * Out <position,id:title:oldPosition>
         **/

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
        /**
         * Il terzo mapper recupera i valori del contatore relativo al gruppo di film con
         * rating dell'anno precedente maggiore di 1 rispetto al rating corrente:
         * se il film ha rating k controlla il valore di count_cumulative[k-1] ossia il minimo
         * numero di film che lo precedono.
         * Preso quel valore lo si somma alla posizione locale,
         * ossia quella all'interno del gruppo di film con la stessa votazione.
         * Out <newRate,id:title:oldPosition>
         **/


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
        /**
         * Il secondo mapper ha il compito di aumentare il contatore relativo al rating del film analizzato
         * Se il film ha rating k che è si aumenterà il contatore[50-k]
         * Out <oldRate,id:Title>
         * **/
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
        /**
         *  Il primo mapper ha il compito di prelevare ed etichettare i film ed i rating.
         *  I film avranno un header F che sta per FILM
         *  I rating avranno un header R:L/R:P, dove L sta per LAST e P per PREV (ultimo anno o quello precedente)
         *  In uscita avremo <idFilm,F:Titolo> oppure <idFilm,R:Rating>||<idFilm,R:L:Rating>
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
            Reducer<IntWritable, Text, Text, Text> {
        /** Questo primo reducer ha il compito di associare i ranking (con header R) ai film (con header F).
         *  I rating hanno un header R:L/R:P, dove L sta per LAST e P per PREV (ultimo anno o quello precedente)
         *
         *  Ogni nuovo rating comporta l'aggiornamento della media dei ranking dell'anno corrispondente.
         *  In uscita <oldRating,idFilm:title:lastRanking>
         *
         * **/
        public enum ValueType {RATING, FILM, UNKNOWN, PREV, LAST}

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
        /** Questo secondo reducer aggiunge un trailer con la posizione locale del film all'interno della
         * stessa classe di film con lo stesso rating (relativo all'anno precedente) **/
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            long count = 0;

            for (Text t : values) {

                count++;
                context.write(new Text(key.toString()), new Text(t + ":" + count));
            }
        }
    }
    public static class PostOrderingReducer extends Reducer<IntWritable, Text, Text, Text> {
        /** Questo terzo reducer aggiunge un trailer con la posizione locale del film all'interno della
         * stessa classe di film con lo stesso rating (relativo all'ultimo anno) **/
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            long count = 0;

            for (Text t : values) {
                count++;

                context.write(new Text(key.toString()), new Text(t.toString() + ":" + count));
            }
        }
    }

    public static class FinalOrderingReducer extends Reducer<IntWritable, Text, Text, Text> {
        /** Questo quarto reducer stampa su file **/
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text t : values) {
                String[] splt = t.toString().split(":");
                String title= splt[2];
                for(int j=3;j<splt.length-2;j++){
                    title+=":"+splt[j];
                }
                int delta=Integer.parseInt(splt[splt.length-1])-key.get();

                context.write(new Text(key.toString()), new Text("id:"+splt[0]+
                        "   title:"+title+
                        "   oldRanking:"+splt[splt.length-1]+
                        "   delta:"+delta));
            }

        }

    }

    public static class DatePartitioner extends Partitioner<IntWritable, Text> {

        public int getPartition(IntWritable key, Text value, int numPartitions) {
            return (50 - key.get());
            /**la posizione è inversamente proporzionale alle stelle guadagnate
             * 5.0 stelle = 50 -> partition[0]
             * 4.9 stelle = 49 -> partition[1]
             * ...
             **/
        }
    }
    public static class FinalPartitioner extends Partitioner<IntWritable, Text> {

        public int getPartition(IntWritable key, Text value, int numPartitions) {
            return (key.get()-1)%numPartitions;

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
       // conf.set("fs.defaultFS", "hdfs://127.0.0.1:50070");
       /* conf.set("fs.defaultFS", "hdfs://master:54310");
        conf.set("mapreduce.jobtracker.address", "master:19888");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.address", "localhost:8088");*/

        /** Primo step:
         *      Nella fase di map si generano le tuple lette dai due file di input
         *          riguardanti i film, ed i rating degli stessi effettuati negli ultimi due anni.
         *      Nella fase di reduce si associano le valutazioni dei film ai film stessi,
         *          così è possibile farne la media delle valutazioni. **/
        Job job = Job.getInstance(conf, "Query3step4Hdfs");
        job.setJarByClass(Query3step4Hdfs.class);
        Path unionStage = new Path(args[2] + "_union");
        Path preOldPositioning = new Path(args[2] + "_preOldPositioning");
        Path oldPositioning = new Path(args[2]+"_oldPositioning");
        Path finalResult = new Path(args[2]);


        job.setMapOutputKeyClass(IntWritable.class);
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

        long startJob= System.currentTimeMillis();
        int code = job.waitForCompletion(true) ? 0 : 1;
        long finishJob =System.currentTimeMillis()-startJob;
        System.out.println("Tempo di esecuzione Query3 1°Map Reduce: "+finishJob+" ms");
        /**
         * Secondo step:
         *  Nella fase di map si contano il numero di film a parità di rating.
         *  Nella fase di reduce si aggiunge trail con la posizione locale all'interno
         *      dello stesso gruppo di appartenenza (i gruppi sono divisi in base al valore
         *      del vecchio rating)
         * **/
        if (code == 0) {

            Job orderJob = Job.getInstance(conf, "Query3step4Hdfs");

            orderJob.setJarByClass(Query3step4Hdfs.class);

            orderJob.setMapperClass(PreOldMapper.class);
            orderJob.setMapOutputKeyClass(IntWritable.class);
            orderJob.setMapOutputValueClass(Text.class);
            orderJob.setReducerClass(OrderingPhaseReducer.class);
            orderJob.setNumReduceTasks(51);
            orderJob.setOutputKeyClass(Text.class);
            orderJob.setOutputValueClass(Text.class);

            orderJob.setPartitionerClass(DatePartitioner.class);


            orderJob.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(orderJob, unionStage);

            orderJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputPath(orderJob, preOldPositioning);


            long startJob2= System.currentTimeMillis();
            code = orderJob.waitForCompletion(true) ? 0 : 2;
            long finishJob2 =System.currentTimeMillis()-startJob2;
            System.out.println("Tempo di esecuzione Query3 2°MapReduce: "+finishJob2+" ms");


            Counters cs = orderJob.getCounters();
            for (int i = 0; i < 51; i++) {
                Counter c = cs.findCounter("SINGLE_COUNT", "" + i);
            }
            Job positioningJob = Job.getInstance(conf, "Query3step4Hdfs");
            /** Si aggiornano i contatori presi nella fase di mapper con il loro valore cumulativo:
             *  Alla fine del processo count[k] avrà il valore pari alla somma di tutti i film con
             *  rating >=k
             * **/
            positioningJob.getConfiguration().setLong("0", cs.findCounter("SINGLE_COUNT", "0").getValue());
            for (int i = 1; i < 51; i++) {

                Counter c = cs.findCounter("SINGLE_COUNT", "" + i);
                c.increment(cs.findCounter("SINGLE_COUNT", "" + (i - 1)).getValue());
                positioningJob.getConfiguration().setLong(c.getName(), c.getValue());

            }
            /**Nel terzo step si ordinano i film in base al rating relativo all'anno precedente.
             * La posizione dell'anno precedente dipende dal rating del film e dall'ordine in cui
             * il reducer riceve le tuple dal partitioner.
             **/
            if (code == 0) {

                positioningJob.setJarByClass(Query3step4Hdfs.class);

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


                Job lastJob = Job.getInstance(conf, "Query3step4Hdfs");
                cs = positioningJob.getCounters();
                lastJob.getConfiguration().setLong("0", cs.findCounter("SINGLE_COUNT", "0").getValue());
                for (int i = 1; i < 51; i++) {

                    Counter c = cs.findCounter("SINGLE_COUNT", "" + i);

                    c.increment(cs.findCounter("SINGLE_COUNT", "" + (i - 1)).getValue());
                    lastJob.getConfiguration().setLong(c.getName(), c.getValue());

                }

                /**Nel quarto step si ordinano i film in base al rating relativo all'ultimo anno.
                 * La posizione dell'ultimo anno dipende dal rating del film e dall'ordine in cui
                 * il reducer riceve le tuple dal partitioner. Se ne prendono i primi 10.
                 * **/
                if (code == 0) {

                    lastJob.setJarByClass(Query3step4Hdfs.class);

                    lastJob.setMapperClass(LastMapper.class);
                    lastJob.setMapOutputKeyClass(IntWritable.class);
                    lastJob.setMapOutputValueClass(Text.class);
                    lastJob.setReducerClass(FinalOrderingReducer.class);
                    lastJob.setNumReduceTasks(1);
                    lastJob.setOutputKeyClass(Text.class);
                    lastJob.setOutputValueClass(Text.class);

                    lastJob.setPartitionerClass(FinalPartitioner.class);


                    lastJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");

                    lastJob.setInputFormatClass(SequenceFileInputFormat.class);
                    SequenceFileInputFormat.setInputPaths(lastJob, oldPositioning);
                    TextOutputFormat.setOutputPath(lastJob, finalResult);


                    try {
                        long StartQuery3=System.currentTimeMillis();
                        code = lastJob.waitForCompletion(true) ? 0 : 4;

                        long FinishQuery3= System.currentTimeMillis()-StartQuery3;
                        System.out.println("Tempo esecuzione Query3 4°MapReduce"+ FinishQuery3+" ms");

                        System.out.println("Tempo totale esecuzione Query3 (HDFS): "+(finishJob+finishJob2+finishJob3+FinishQuery3)+" ms");

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
