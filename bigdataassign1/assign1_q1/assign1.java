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


public class assign1 {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		if(args.length!=2)
		{
			System.err.println("Usage: hadoop jar <jarFile> ClassName <listof_Friends_adjacency_list.txt path> <output outputpath>");
			System.exit(-1);
		}
		 Configuration conf = new Configuration();
	        Job job = new Job(conf,"Mutual list of friends");
	        job.setJarByClass(assign1.class);
	        job.setMapperClass(Map.class);
	        job.setReducerClass(SReducer.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path((args[1])));
	        System.exit(job.waitForCompletion(true)? 0:-1);
	        //System.exit(job.waitForCompletion(true) ? 0 : 1);	
		
	}
	
	
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
    						//context.write(key, value);
    			}
    		}
    	}
    }
    
    public static class SReducer extends Reducer<Text, Text, Text, Text> {
        //reducer which checks of the common friend already exists in map or not. if not exist then create as mutual friend.
        //structure(output) f1,f2\tmutal friend list
        public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException{
            Text result_mutuals = new Text();
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            StringBuilder new_String= new StringBuilder();
            for(Text data_res: values){
                List<String> value = Arrays.asList(data_res.toString().split(","));
                for(String val_check: value){
                    if(map.containsKey(val_check) ){ //if "A" is in friend list of two friends, then check if it exists in map, else add him to map
                    	//then if it occurs again, append the string.
                        new_String.append(val_check+",");
                    }else{
                        map.put(val_check, 1);
                    }
                }
            }
            
            //remove last(rightmost) occurance of ","from the string to display final list
            if(new_String.lastIndexOf(",")>=1){
                new_String.deleteCharAt(new_String.lastIndexOf(","));
            }
            
            result_mutuals.set(new Text(new_String.toString()));
            if(key.toString().equals("0,4")||key.toString().equals("20,22939")||key.toString().equals("1,29826")||key.toString().equals("6222,19272")||key.toString().equals("28041,28056")) {
            	context.write(key, result_mutuals);
            }
            
        }
    }
}