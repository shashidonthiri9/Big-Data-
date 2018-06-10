package assign1_4;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class assign1_4 {		
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Using Secondary sort to find MinMaxMedian");
		job.setJarByClass(assign1_4.class);
		job.setPartitionerClass(NaturalKeyPartitioner.class);
		job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
		job.setMapOutputKeyClass(check_class.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(Mapper_1.class);
		job.setReducerClass(Reducer_1.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path((args[1])));
		System.exit(job.waitForCompletion(true)? 0:-1);
	}	
	public	static class check_class implements WritableComparable<check_class>{
		public Long id,val;
		public check_class() {
			
		}
		public check_class(long id, long val) {
			super();
			this.id = id;
			this.val = val;
		}
		//getters and setters
		public Long getVal() {
			return val;
		}
		public Long getId() {
			return this.id;
		}
		public void setVal(long val) {
			this.val = val;
		}
		@Override
		public void write(DataOutput arg_1) throws IOException {
			arg_1.writeLong(id);
			arg_1.writeLong(val);
		}
		@Override
		public void readFields(DataInput arg_1) throws IOException {
			id = arg_1.readLong();
			val = arg_1.readLong();
		}
		@Override
		public int compareTo(check_class obj) {
			return (int) (this.getVal()-obj.getVal());
		}
		
		
	}	
	public static class Mapper_1 extends Mapper<LongWritable, Text, check_class, LongWritable>{
    	//public void map(LongWritable Key,Text value, Context context) throws IOException, InterruptedException {
    		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    			final LongWritable check_1 = new LongWritable(1);
    			String str_line = value.toString();
    			long long_val= Long.parseLong(str_line);
    			check_class tmp = new check_class(1,long_val);
    			LongWritable long_writable= new LongWritable();
    			long_writable.set(long_val);
    			if(str_line!=null) {
    			//	context.write(key, value);
    				context.write(tmp, long_writable);
    			}
    		}
    	}
    	
	
	public static class Reducer_1 extends Reducer<check_class,LongWritable, Text, LongWritable>{
		public void reduce(check_class key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException{
			Text check_1 = new Text();
			long trigger=0,finalmedianval;
			Long minval = Long.MAX_VALUE,maxval = Long.MIN_VALUE;
			Map<Long, Long> map = new HashMap<Long, Long>();
			for(LongWritable val: values) {
				trigger++;
				map.put(trigger, val.get());
				if(minval>val.get()) {
					minval = val.get();
				}
				if(maxval<val.get()) {
					maxval = val.get();
				}	
			}
			String finalminVal = minval.toString(),finalmaxVal = maxval.toString();
			
			if(trigger%2==0) {
				long floor_fn = Math.floorDiv(trigger, 2),ceil_fn = (long) Math.ceil(trigger/ 2);
				finalmedianval = (map.get(floor_fn)+map.get(ceil_fn))/2;
			}else {
				long ceil_fn = (long) Math.ceil(trigger/ 2);
				finalmedianval = map.get(ceil_fn);
			}
			LongWritable medianValue = new LongWritable();
			medianValue.set(finalmedianval);
			context.write(new Text(new String("min_value:"+finalminVal+"	"+"max_value:"+finalmaxVal+"	median_value:")), medianValue);
		}	}
	
	public static class NaturalKeyPartitioner extends Partitioner<check_class, LongWritable>{
//	public static class NaturalKeyPartitioner extends Partitioner<box_classtemporary, LongWritable>
		@Override
		public int getPartition(check_class key, LongWritable value, int no_of_partitions) {
			int hash = key.getId().hashCode(),partition_val = hash % no_of_partitions;
			return partition_val;
		}
	}
	public static class NaturalKeyGroupingComparator extends WritableComparator{
		protected NaturalKeyGroupingComparator() {
			super(check_class.class, true);
		}
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable wc1, WritableComparable wc2) {
			check_class _obj1 = (check_class)wc1;
			check_class _obj2 = (check_class)wc2;
			return _obj1.getId().compareTo(_obj2.getId());
		}	
	}
}