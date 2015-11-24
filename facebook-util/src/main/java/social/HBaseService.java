package social;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.social.facebook.api.Post;
import org.springframework.social.facebook.api.User;
import org.springframework.stereotype.Service;

@Service
public class HBaseService {

	@Autowired
	private HbaseTemplate hbaseConfig;

	private final TableName FEED_TABLE = TableName.valueOf("feed");
	private final TableName OUTPUT_TABLE = TableName.valueOf("output");

	private final byte[] RESULT = Bytes.toBytes("result");
	private final byte[] NAME = Bytes.toBytes("name");
	private final byte[] POST = Bytes.toBytes("post");
	private final byte[] ID = Bytes.toBytes("id");
	private final byte[] MESSAGE = Bytes.toBytes("message");
	private final byte[] VALUE = Bytes.toBytes("value");

	public void upload(User profile, List<Post> feed) throws IOException {
		try (Connection connection = ConnectionFactory.createConnection(hbaseConfig.getConfiguration())) {
			uploadFeed(connection, profile, feed);
			uploadOutput(connection, profile);
		}
	}

	public List<User> scanResultList() throws IOException {
		List<User> resultList = new ArrayList<>();
		try (Connection connection = ConnectionFactory.createConnection(hbaseConfig.getConfiguration())) {
			try (HTable hTable = (HTable) connection.getTable(OUTPUT_TABLE)) {
				Scan scan = new Scan();
				scan.addColumn(RESULT, NAME);
				try (ResultScanner scanner = hTable.getScanner(scan)) {
					for (Result result = scanner.next(); result != null; result = scanner.next()) {
						byte[] id = result.getRow();
						byte[] name = result.getValue(RESULT, NAME);
						User profile = new User(Bytes.toString(id), Bytes.toString(name), null, null, null, null);
						resultList.add(profile);
					}
				}
			}
		}
		return resultList;
	}

	public User getResult(String id) throws IOException {
		User user = null;
		try (Connection connection = ConnectionFactory.createConnection(hbaseConfig.getConfiguration())) {
			try (HTable hTable = (HTable) connection.getTable(OUTPUT_TABLE)) {
				Get g = new Get(Bytes.toBytes(id));
				Result result = hTable.get(g);
				byte[] name = result.getValue(RESULT, NAME);
				byte[] value = result.getValue(RESULT, VALUE);
				user = new User(id, Bytes.toString(name), Bytes.toString(value), null, null, null);
			}
		}
		return user;
	}

	private void uploadFeed(Connection connection, User profile, List<Post> feed) throws IOException {
		try (HTable hTable = (HTable) connection.getTable(FEED_TABLE)) {
			List<Put> puts = new ArrayList<>(feed.size());
			for (Post post : feed) {
				Put put = new Put(Bytes.toBytes(post.getId()));
				put.addColumn(POST, ID, Bytes.toBytes(profile.getId()));
				put.addColumn(POST, MESSAGE, Bytes.toBytes(post.getMessage()));
				puts.add(put);
			}
			hTable.put(puts);
		}
	}

	private void uploadOutput(Connection connection, User profile) throws IOException {
		try (HTable hTable = (HTable) connection.getTable(OUTPUT_TABLE)) {
			Put put = new Put(Bytes.toBytes(profile.getId()));
			put.addColumn(RESULT, NAME, Bytes.toBytes(profile.getName()));
			hTable.put(put);
		}
	}

}