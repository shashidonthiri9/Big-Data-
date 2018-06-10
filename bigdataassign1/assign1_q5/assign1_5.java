package assign1_5;
import java.awt.List;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class assign1_5 {
	static class Matrix_multiplication_map extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] data_vals = value.toString().split(",");
			String pairwise_data = new String();
			String Column_row = new String();
			if(data_vals[0].trim().equals("A")) {
				Column_row  = data_vals[2]; 
				pairwise_data = "A" + ","+ data_vals[1].trim()+ ","+ data_vals[3].trim();
			}else if(data_vals[0].trim().equals("B")) {
				Column_row = data_vals[1];
				pairwise_data = "B" + ","+ data_vals[2].trim()+ ","+ data_vals[3].trim();
			}
			context.write(new Text(Column_row), new Text(pairwise_data));
		}
	}
	static class Matrix_multiplication_reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable <Text> listPairs, Context context) throws IOException, InterruptedException {
			ArrayList<String> list_ = new ArrayList<String>();
			for(Text pairwise_datas: listPairs) {
				list_.add(pairwise_datas.toString());
			}
			for(int i=0;i<list_.size();i++) {
				String[] val_str1 = list_.get(i).split(",");
				System.out.println("here value1 is"+list_.get(i).toString());
				if(val_str1[0].equals("A")) {

					String row = val_str1[1];String rowVal = val_str1[2];
					for(int j=0;j<list_.size();j++) {
						String pairwise_data = new String();
						String[] val_str2= list_.get(j).split(",");
						String col = val_str2[1]; String colVal = val_str2[2];
						if(val_str2[0].equals("B")) {
							System.out.println("here value2 is"+list_.get(j).toString());
							pairwise_data = row+"_"+col;
							int mul_val = (Integer.parseInt(rowVal)*Integer.parseInt(colVal));
							String value = String.valueOf(mul_val);
							context.write(new Text(pairwise_data),new Text(value));
						}
					}
				}
			}

		}


	}
	static class Matrix_multiplication_map1 extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] data_vals = value.toString().split("\t");
			IntWritable map_val = new IntWritable();
			if(data_vals.length==2) {
				String map_key = data_vals[0];
				String val = data_vals[1];
				int mapValue = Integer.parseInt(val);
				map_val.set(mapValue);
				context.write(new Text(map_key), map_val);
			}
		}
	}

	static class Matrix_multiplication_reduce1 extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text mapKey, Iterable <IntWritable> listof_Multi_Values, Context context) throws IOException, InterruptedException {
			int val=0;IntWritable mul_val = new IntWritable();
			for(IntWritable value: listof_Multi_Values) {
				val = val + value.get();
			}
			mul_val.set(val);
			context.write(mapKey,mul_val);
		}
	}


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.out.println("Usage: hadoop jar <jarFile> ClassName <soc-LiveJournal1Adj.txt path> <output folder path1 for mapreduce1> <Final Output Path>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Matrix multiplication job 1");
		job.setJarByClass(assign1_5.class);
		job.setMapperClass(Matrix_multiplication_map.class);
		job.setReducerClass(Matrix_multiplication_reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//using job chaining to send first output to the second mapper.
		if (job.waitForCompletion(true)) {
			Configuration conf1 = new Configuration();
			Job job2 = new Job(conf1, "Matrix multiplication job 2");
			job2.setJarByClass(assign1_5.class);
			job2.setMapperClass(Matrix_multiplication_map1.class);
			job2.setReducerClass(Matrix_multiplication_reduce1.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);
			//first mapreduce output as input for second mapreduce.
			FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
		}
	}