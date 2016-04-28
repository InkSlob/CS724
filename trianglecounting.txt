package org.myorg;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class TriangleCounting {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
      private Text outValueText = new Text();
	  private Text outKeyText = new Text();
      private ArrayList<String> neighbors = new ArrayList<String>();
	  private String vertex;

      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String line = value.toString();
		String[] parts=line.split(" ");
		if (parts.length<=0)
			return;
		vertex=parts[0];
		
        String[] neighbors=parts[1].split(",");
		outValueText.set("-1"); 	
		for (int i=0; i<neighbors.length; i++) {
			outKeyText.set(vertex+" "+neighbors[i]);
			output.collect(outKeyText, outValueText);
		}
		outValueText.set(vertex);
		
		for (int i=0; i<neighbors.length-1; i++) {
			for (int j=i+1; j<neighbors.length; j++) {
				outKeyText.set(neighbors[i]+" "+neighbors[j]);
				output.collect(outKeyText, outValueText);
			}
		}
      }
    }
	
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private Text outValueText = new Text();
		private Text outKeyText = new Text();
      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        int numTrgls = 0;
		boolean isEdge=false;
		String[] vertices=key.toString().split(" "); 
		int v1=Integer.parseInt(vertices[0]); 
		int v2=Integer.parseInt(vertices[1]);
		ArrayList<Integer> neighbors = new ArrayList<Integer>();
		while (values.hasNext()) {
			String temp=values.next().toString();
			if (temp.equals("-1"))
				isEdge=true;
			else
				neighbors.add(Integer.parseInt(temp));
		}
		if (isEdge==false)  //(v1, v2) is not an edge
			return;
		
		String keyString=key.toString();
		for (int i=0; i<neighbors.size(); i++) {
			if (neighbors.get(i)<v1 && neighbors.get(i)<v2) {
				String outValue=Integer.toString(neighbors.get(i))+" "+keyString;
				outValueText.set(outValue);
				output.collect(outKeyText, outValueText); 
			}
		}
      }
    }
	
    public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(TriangleCounting.class);
      conf.setJobName("triangleCounting");

      conf.setOutputKeyClass(Text.class);
	  conf.setMapOutputValueClass(Text.class);
      conf.setOutputValueClass(Text.class);

      conf.setMapperClass(Map.class);
      //conf.setCombinerClass(Reduce.class);
      conf.setReducerClass(Reduce.class);

      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));

      JobClient.runJob(conf);
    }
}
