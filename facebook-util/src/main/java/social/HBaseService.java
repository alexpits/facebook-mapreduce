package social;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
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

	public void upload(User profile, List<Post> feed) throws IOException {
		try (Connection connection = ConnectionFactory.createConnection(hbaseConfig.getConfiguration())) {
			uploadFeed(connection, profile, feed);
			uploadOutput(connection, profile);
		}
	}

	private void uploadFeed(Connection connection, User profile, List<Post> feed) throws IOException {
		try (HTable hTable = (HTable) connection.getTable(FEED_TABLE)) {
			List<Put> puts = new ArrayList<Put>(feed.size());
			for (Post post : feed) {
				Put put = new Put(Bytes.toBytes(post.getId()));
				put.addColumn(Bytes.toBytes("post"), Bytes.toBytes("id"), Bytes.toBytes(profile.getId()));
				put.addColumn(Bytes.toBytes("post"), Bytes.toBytes("message"), Bytes.toBytes(post.getMessage()));
				puts.add(put);
			}
			hTable.put(puts);
		}
	}

	private void uploadOutput(Connection connection, User profile) throws IOException {
		try (HTable hTable = (HTable) connection.getTable(OUTPUT_TABLE)) {
			Put put = new Put(Bytes.toBytes(profile.getId()));
			put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("name"), Bytes.toBytes(profile.getName()));
			hTable.put(put);
		}
	}

}