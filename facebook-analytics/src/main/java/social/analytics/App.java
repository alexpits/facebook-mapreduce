package social.analytics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class App {

	// Tables
	public static final TableName FEED_TABLE = TableName.valueOf("feed");
	public static final TableName PROFILE_TABLE = TableName.valueOf("profile");
	public static final TableName WORDCOUNT_TABLE = TableName.valueOf("wordcount");

	// Column families
	public static final byte[] CF_USER = Bytes.toBytes("user");
	public static final byte[] CF_POST = Bytes.toBytes("post");
	public static final byte[] CF_WORD = Bytes.toBytes("word");

	// Columns
	public static final byte[] USERID = Bytes.toBytes("userid");
	public static final byte[] USERNAME = Bytes.toBytes("username");
	public static final byte[] MESSAGE = Bytes.toBytes("message");
	public static final byte[] VALUE = Bytes.toBytes("value");
	public static final byte[] COUNT = Bytes.toBytes("count");
	public static final byte[] JOBSTATUS = Bytes.toBytes("status");

	private Configuration config = HBaseConfiguration.create();

	public App() {
	}

	public static void main(String[] args) {
		try {
			App app = new App();
			app.run();
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void run() throws IOException, InterruptedException {
		String[] jobIds = getJobIds();
		List<ControlledJob> dependingJobs = new ArrayList<>();
		String groupName = Long.toString(System.currentTimeMillis());
		JobControl jobControl = new JobControl(groupName);
		for (String jobId : jobIds) {
			ControlledJob job = getJob(jobId, dependingJobs);
			jobControl.addJob(job);
		}
		Thread thread = new Thread(jobControl);
		thread.start();

		while (!jobControl.allFinished()) {
			Thread.sleep(5000);
		}

		setDone(jobIds);
	}

	private String[] getJobIds() throws IOException {
		List<String> jobIds = new ArrayList<>();
		try (Connection connection = ConnectionFactory.createConnection(config)) {
			try (HTable hTable = (HTable) connection.getTable(App.PROFILE_TABLE)) {
				SingleColumnValueFilter jobFilter = new SingleColumnValueFilter(App.CF_USER, App.JOBSTATUS,
						CompareOp.EQUAL, Bytes.toBytes(0));
				Scan scan = new Scan();
				scan.setFilter(jobFilter);
				scan.addColumn(App.CF_USER, App.USERID);
				scan.addColumn(App.CF_USER, App.JOBSTATUS);
				scan.setCaching(500);

				try (ResultScanner scanner = hTable.getScanner(scan)) {
					for (Result result = scanner.next(); result != null; result = scanner.next()) {
						byte[] jobId = result.getRow();
						jobIds.add(Bytes.toString(jobId));
					}
				}
			}
		}

		return jobIds.toArray(new String[jobIds.size()]);
	}

	private void setDone(String[] jobIds) throws IOException {
		List<Put> puts = new ArrayList<>(jobIds.length);
		for (String jobId : jobIds) {
			Put put = new Put(Bytes.toBytes(jobId));
			put.addColumn(App.CF_USER, App.JOBSTATUS, Bytes.toBytes(1));
			puts.add(put);
		}
		try (Connection connection = ConnectionFactory.createConnection(config)) {
			try (HTable hTable = (HTable) connection.getTable(App.PROFILE_TABLE)) {
				hTable.put(puts);
			}
		}
	}

	private ControlledJob getJob(String userId, List<ControlledJob> dependingJobs) throws IOException {
		Job job = Job.getInstance(config, userId);
		job.setJarByClass(AnalyticsJob.class);

		SingleColumnValueFilter filter = new SingleColumnValueFilter(App.CF_POST, App.USERID, CompareOp.EQUAL,
				Bytes.toBytes(userId));

		Scan scan = new Scan();
		scan.addColumn(App.CF_POST, App.USERID);
		scan.addColumn(App.CF_POST, App.MESSAGE);
		scan.setCaching(500);
		scan.setFilter(filter);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob(App.FEED_TABLE, scan, AnalyticsJob.PostsMapper.class, Text.class,
				IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(App.WORDCOUNT_TABLE.toString(), AnalyticsJob.PostsReducer.class, job);
		job.setNumReduceTasks(1);

		return new ControlledJob(job, dependingJobs);
	}

}
