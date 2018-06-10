package assign1_3;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class assign1_3 {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		if(args.length!=2)
		{
			System.err.println("Usage: hadoop jar <jarFile> ClassName <listof_Friends_adjacency_list.txt path> <output outputpath>");
			System.exit(-1);
		}
		 Configuration conf = new Configuration();
	        Job job = new Job(conf,"Mean Variance");
	        job.setJarByClass(assign1_3.class);
	        job.setMapperClass(Map.class);
	        job.setReducerClass(SReducer.class);
	        job.setMapOutputKeyClass(LongWritable.class);
	        job.setMapOutputValueClass(LongWritable.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path((args[1])));
	        System.exit(job.waitForCompletion(true)? 0:-1);
	        //System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
	
	public static class Map extends Mapper<LongWritable, Text, LongWritable, LongWritable>{
    	public void map(LongWritable Key,Text value, Context context) throws IOException, InterruptedException {
    		LongWritable check_=new LongWritable(1);
    		String string_line=value.toString();
    		long long_Val=Long.parseLong(string_line);
    		LongWritable long_w=new LongWritable();
    		long_w.set(long_Val);
    		if(string_line!=null) {
    			context.write(check_,long_w);
    		}
    		
    		//				context.write(string_line, new Text(data_key[1]));
    						
    			}
    		}
    	
    
    
    public static class SReducer extends Reducer<LongWritable, LongWritable, Text, Text> {
        public void reduce(LongWritable key, Iterable <LongWritable> values, Context context) throws IOException, InterruptedException{
        	long sum_val=0,cnt_val=0,sq_Val=0;
        	String Str_cnt=new String();
        	String sum_str=new String();
        	String sqr_str=new String();
        	for(LongWritable value: values) {
				sum_val = sum_val + value.get();
				sq_Val = sq_Val+ value.get()*value.get();
				cnt_val= cnt_val+1;
			}
			sum_str = Long.toString(sum_val);
			Str_cnt = Long.toString(cnt_val);
			sqr_str = Long.toString(sq_Val);
			long mean = sum_val/cnt_val;
			long variance = sq_Val/cnt_val- (mean*mean); //E(x^2)-(E(x))^2
			String Mean_Val = Long.toString(mean);
			String finalVariance = Long.toString(variance);

			context.write(new Text(Mean_Val), new Text(finalVariance ));

                    }
                }
            
            
               }
