package social.analytics;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class AnalyticsJob {

	private static final String CLEANUP_REGEX = "[^\\p{L}\\p{Nd}]+";
	private static final String HTTP = "http";

	public static class PostsMapper extends TableMapper<Text, IntWritable> {

		private final IntWritable ONE = new IntWritable(1);
		private Text text = new Text();

		public void map(ImmutableBytesWritable row, Result value, Context context)
				throws IOException, InterruptedException {
			String val = new String(value.getValue(App.CF_POST, App.MESSAGE));
			StringTokenizer tok = new StringTokenizer(val);
			while (tok.hasMoreTokens()) {
				String key = tok.nextToken().replaceAll(CLEANUP_REGEX, "");
				if (!key.startsWith(HTTP) && key.length() != 0) {
					text.set(key);
					context.write(text, ONE);
				}
			}
		}

	}

	public static class PostsReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				count += val.get();
			}

			Put put = new Put(Bytes.toBytes(key.toString() + context.getJobName()));
			put.addColumn(App.CF_WORD, App.USERID, Bytes.toBytes(context.getJobName()));
			put.addColumn(App.CF_WORD, App.VALUE, Bytes.toBytes(key.toString()));
			put.addColumn(App.CF_WORD, App.COUNT, Bytes.toBytes(count));

			context.write(null, put);
		}
	}

}
