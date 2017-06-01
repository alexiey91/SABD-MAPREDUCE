package query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import util.HBaseClient;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by root on 30/05/17.
 */
public class Cane {

public Cane(){}
    public static  void main(String[] args){
        try{
            HBaseClient hbase = new HBaseClient();


            if( !hbase.exists("Query"))
            hbase.createTable("Query", "movie","rating","value");

            Path pt=new Path("hdfs://master:54310/OutputQuery1/part-r-00000");

           // System.out.println("path"+pt);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line= "";
            line=br.readLine();
            //System.out.println("line: "+line);
            int i=0;
            while (line != null){
                System.out.println("line: "+line);

                String[] parts = line.split(Pattern.quote( "#"));
                System.out.println("parts[0]"+parts[0]);
                String[] colonnaMovies = {"movie","col1",parts[0]};
               // List<String> colonnaMovie = new ArrayList<String>();
                //colonnaMovie.add("movieCol");
                // colonnaMovie.add("col1");
               // colonnaMovie.add(parts[0]);
               // System.out.println("parts[1]"+parts[1]);



                hbase.put("Query","row1","movie","col"+i,parts[0]);
                String[] subparts = parts[1].split(Pattern.quote(";"));
                hbase.put("Query","row1","rating","col"+i, subparts[0]);
                hbase.put("Query","row1","value", "col"+i,subparts[1]);
                i++;
                line=br.readLine();


            }
        }catch(Exception e){
            System.err.println(e);
        }

    }
}
