package assign1_q2;
import java.io.IOException;
import java.security.KeyStore.Entry;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class q2 {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
        String[] All_Args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (All_Args.length != 3) {
            System.out.println("Usage: hadoop jar <jarFile> ClassName <soc-LiveJournal1Adj.txt path> <output folder path1 for mapreduce1> <Final Output Path>");
            System.exit(-1);
        }
		// Configuration conf = new Configuration();
//	        Job job = new Job(conf,"Mutual list of friends");
	        Job job = Job.getInstance(conf, "Mutual Friends");
	        job.setJarByClass(q2.class);
	        job.setMapperClass(Map.class);
	        job.setReducerClass(SReducer.class);
	        job.setMapOutputKeyClass(Text.class);
	        //job.setOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(Text.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        FileInputFormat.addInputPath(job, new Path(All_Args[0]));
	        FileOutputFormat.setOutputPath(job, new Path((All_Args[1])));
	        
	        if(job.waitForCompletion(true)) {
	        	Configuration conf_1=new Configuration();
	        	Job job2=new Job(conf_1,"top 10 list");
	        	job2.setJarByClass(q2.class);
	        	job2.setMapperClass(Map_1.class);
	        	job2.setReducerClass(SReducer_1.class);
	        	
	        //	job2.setInputFormatClass(TextInputFormat.class);
	        	job2.setMapOutputKeyClass(IntWritable.class);
	        	job2.setMapOutputValueClass(Text.class);
	        	job2.setOutputKeyClass(Text.class);
	        	job2.setOutputValueClass(IntWritable.class);
	        	FileInputFormat.addInputPath(job2,new Path(All_Args[1]));
	        	//FileOutputFormat.setOutputCompressorClass(job2, codecClass);
	        	FileOutputFormat.setOutputPath(job2,new Path(All_Args[2]) );
	        	System.exit(job2.waitForCompletion(true)? 0:-1);
	        }
	        //System.exit(job.waitForCompletion(true)? 0:-1);
	        //System.exit(job.waitForCompletion(true) ? 0 : 1);	
		
	}
	
	
	//public static class Map_1 extends Mapper<LongWritable , Text, >
	
	
	   //create a mapper for each friend in the friend list(sort based on the order so no duplicates(f1, f2) == (f2, f1)
    //structure: f1,f2\tf2,f3,f4.....
    //send to reducer.
    public static class Map extends Mapper<LongWritable, Text, Text, Text>{
    	public void map(LongWritable Key,Text value, Context context) throws IOException, InterruptedException {
    		Text string_line=new Text(); //string to be created on appending each keyvalue
    		String data_key[]=value.toString().split("\t"); //key separated by \t follwoed by then list of values
    		if(data_key.length==2) {                        
    			String key1=data_key[0];                    //assign key
    			List<String> data_values =Arrays.asList(data_key[1].split(",")); //list of value separated by "," as delimiter
    			for(String key2:data_values) {
    				int key1val=Integer.parseInt(key1);
    				int key2val=Integer.parseInt(key2);
    				if(key1val<key2val) {
    					string_line.set(key1val+","+key2val);
    									}
    				else {
    					string_line.set(key2val+","+key1val);
    				}
    						context.write(string_line, new Text(data_key[1]));
    						
    			}
    		}
    	}
    }
    
    public static class SReducer extends Reducer<Text, Text, Text, IntWritable> {
        //reducer which checks of the common friend already exists in map or not. if not exist then create as mutual friend.
        //structure(output) f1,f2\tmutal friend list
        public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException{
            //Text result_mutuals = new Text();
            int count=0;
            HashMap<String, Integer> map = new HashMap<String, Integer>();
           // StringBuilder new_String= new StringBuilder();
            for(Text data_res: values){
                List<String> value = Arrays.asList(data_res.toString().split(","));
                for(String val_check: value){
                    if(map.containsKey(val_check)){ //if "A" is in friend list of two friends, then check if it exists in map, else add him to map
                    	//then if it occurs again, append the string.
                       // new_String.append(val_check+",");
                       count+=1; 
                    }else{
                        map.put(val_check, 1);
                  }
                }
            }            
            //remove last(rightmost) occurance of ","from the string to display final list
//            if(new_String.lastIndexOf(",")>=1){
//                new_String.deleteCharAt(new_String.lastIndexOf(","));
//            }
//            result_mutuals.set(new Text(new_String.toString()));
            context.write(key, new IntWritable(count));
        }
    }
    
    public static class Map_1 extends Mapper<LongWritable ,Text, IntWritable,Text>{
    	private final static IntWritable one=new IntWritable(1);
    	public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException
    	{
    		context.write(one, value);    	
    	}
    }
    
    public static class compare_values implements Comparator {
HashMap map;
		
		public compare_values(HashMap map) {
			// TODO Auto-generated method stub
			this.map=map;
		}

		@Override
		public int compare(Object o1, Object o2) {
			// TODO Auto-generated method stub
			Integer v1=(Integer)map.get(o1);
			Integer v2=(Integer)map.get(o2);
			if((v1==v2) || (v2 >v1))
			{
			return 1;
			}
			else {
				return -1;
			}
		}
    }
    
    public static class SReducer_1 extends Reducer<IntWritable,Text,Text,IntWritable>{
    	public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            for(Text val_check: values){
                String[] read_line = val_check.toString().split("\t");
                String s = read_line[0];
                Integer i = Integer.parseInt(read_line[1]);
                //insert values to hashmap
                map.put(s, i);
            }
            //sort the map using treemap
            TreeMap<String, Integer> sorted =  new TreeMap<String, Integer>(new compare_values(map));
            sorted.putAll(map);
            int count = 0;
            for(java.util.Map.Entry<String, Integer> entry: sorted.entrySet()){
                //write values to list.
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
                count++;
                //write till count<10
                if(count==10){
                    break;
                }
            }
        }
    }
}