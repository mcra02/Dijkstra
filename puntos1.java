import java.io.IOException;
//import java.nio.file.FileSystem;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import java.util.ListIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class puntos extends Configured implements Tool {
    public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text loc = new Text();
        private Text rating = new Text();

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {

            List<String> elm = new ArrayList<String>();

            String[] rows = value.toString().split(":");
            String[] punt = rows[2].split("-");

            elm.add("A:0");

            int num = Integer.parseInt(rows[1]);

            for (int i = 0; i < punt.length; i++) {
                int suma = num;
                String[] val = punt[i].split(",");
                int nump = Integer.parseInt(val[1]);
                suma = suma + nump;
                elm.add(val[0].concat(":").concat(String.valueOf(suma)));
            }

            for (String cor : elm) {
                String[] cord = cor.split(":");
                output.collect(new Text(cord[0]), new Text(cord[1]));
            }

        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            String result = "";
            String valor = "";
            int menor = 1000;
            while (values.hasNext()) {
                valor = values.next().toString();
                int num = Integer.parseInt(valor);
                if (num < menor) {
                    menor = num;
                }
            }

            output.collect(key, new Text(String.valueOf(menor)));
        }
    }

    static int printUsage() {
        System.out.println("CaminoMasCorto [-m <maps>] [-r <reduces>] <input> <output>");
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.toString());
    }

    @Override
    public int run(String[] args) throws IOException {

        final Path output = new Path(args[1]);
        FileSystem.get(output.toUri(), getConf()).delete(output, true);

        JobConf conf = new JobConf(puntos.class);
        conf.setJobName("CaminoMasCorto");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(MapClass.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);

        // inicio
        Configuration confi = new Configuration();
        FileSystem fs = FileSystem.get(confi);
        Path inFile0 = new Path("/dijkstra/output/part-00000");
        Path inFile1 = new Path("/dijkstra/output/part-00001");
        Path inFile2 = new Path("/dijkstra/input/puntos.txt");
        Path outFile = new Path("/dijkstra/input/puntos.txt");
        FSDataInputStream in0 = fs.open(inFile0);
        FSDataInputStream in1 = fs.open(inFile1);
        FSDataInputStream in2 = fs.open(inFile2);
        InputStreamReader isr0 = new InputStreamReader(in0);
        BufferedReader br0 = new BufferedReader(isr0);
        InputStreamReader isr1 = new InputStreamReader(in1);
        BufferedReader br1 = new BufferedReader(isr1);
        InputStreamReader isr2 = new InputStreamReader(in2);
        BufferedReader br2 = new BufferedReader(isr2);
        String line0, line1, line2;

        List<String> newkeys = new ArrayList<String>();
        List<String> points = new ArrayList<String>();

        while ((line0 = br0.readLine()) != null) {
            String lin0 = line0.trim();
            String[] keys0 = lin0.split("\t");
            newkeys.add(keys0[0].concat(":").concat(keys0[1]));
        }

        while ((line1 = br1.readLine()) != null) {
            String lin1 = line1.trim();
            String[] keys1 = lin1.split("\t");
            newkeys.add(keys1[0].concat(":").concat(keys1[1]));
        }

        while ((line2 = br2.readLine()) != null) {
            points.add(line2);
        }

        final Path input = new Path(args[0]);
        FileSystem.get(input.toUri(), getConf()).delete(input, true);

        FSDataOutputStream out = fs.create(outFile);

        for (String point : points) {
            String[] data = point.split(":");
            String newdata = "";
            for (String elm : newkeys) {
                String[] nkey = elm.split(":");
                if (data[0].equals(nkey[0])) {
                    newdata = nkey[0].concat(":").concat(nkey[1]).concat(":").concat(data[2]).concat("\n");
                }
            }
            out.writeUTF(newdata);
        }

        in0.close();
        in1.close();
        in2.close();
        out.close();

        // fin

        return 0;
    }

    public static void main(String[] args) throws IOException {

        int x = 1;
        while (x > 0) {
            try {
                ToolRunner.run(new puntos(), args);
            } catch (Exception e) {

            }

            x -= 1;
        }
    }
}