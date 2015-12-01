package social;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
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

	// Tables
	public static final TableName FEED_TABLE = TableName.valueOf("feed");
	public static final TableName PROFILE_TABLE = TableName.valueOf("profile");
	public static final TableName WORDCOUNT_TABLE = TableName.valueOf("wordcount");

	// Column families
	public static final byte[] CF_USER = Bytes.toBytes("user");
	public static final byte[] CF_POST = Bytes.toBytes("post");
	public static final byte[] CF_WORD = Bytes.toBytes("word");

	// Columns
	private static final byte[] USERID = Bytes.toBytes("userid");
	private static final byte[] USERNAME = Bytes.toBytes("username");
	private static final byte[] MESSAGE = Bytes.toBytes("message");
	private static final byte[] VALUE = Bytes.toBytes("value");
	private static final byte[] COUNT = Bytes.toBytes("count");
	private static final byte[] JOBSTATUS = Bytes.toBytes("status");

	private static final byte[] PENDING = Bytes.toBytes(0);

	public void upload(User profile, List<Post> feed) throws IOException {
		try (Connection connection = ConnectionFactory.createConnection(hbaseConfig.getConfiguration())) {
			saveFeed(connection, profile, feed);
			saveProfile(connection, profile);
		}
	}

	public List<User> getUsers() throws IOException {
		List<User> users = new ArrayList<>();
		try (Connection connection = ConnectionFactory.createConnection(hbaseConfig.getConfiguration())) {
			try (HTable hTable = (HTable) connection.getTable(PROFILE_TABLE)) {
				Scan scan = new Scan();
				scan.addColumn(CF_USER, USERNAME);
				try (ResultScanner scanner = hTable.getScanner(scan)) {
					for (Result result = scanner.next(); result != null; result = scanner.next()) {
						byte[] id = result.getRow();
						byte[] name = result.getValue(CF_USER, USERNAME);
						User profile = new User(Bytes.toString(id), Bytes.toString(name), null, null, null, null);
						users.add(profile);
					}
				}
			}
		}
		return users;
	}

	public User getUser(String id) throws IOException {
		User user = null;
		try (Connection connection = ConnectionFactory.createConnection(hbaseConfig.getConfiguration())) {
			try (HTable hTable = (HTable) connection.getTable(PROFILE_TABLE)) {
				Get g = new Get(Bytes.toBytes(id));
				Result result = hTable.get(g);
				byte[] name = result.getValue(CF_USER, USERNAME);
				user = new User(id, Bytes.toString(name), null, null, null, null);
			}
		}
		return user;
	}

	public Map<String, Integer> getWords(String userId, int minCount) throws IOException {
		Map<String, Integer> words = new HashMap<String, Integer>();
		try (Connection connection = ConnectionFactory.createConnection(hbaseConfig.getConfiguration())) {
			try (HTable hTable = (HTable) connection.getTable(WORDCOUNT_TABLE)) {
				SingleColumnValueFilter userFilter = new SingleColumnValueFilter(CF_WORD, USERID, CompareOp.EQUAL,
						Bytes.toBytes(userId));
				SingleColumnValueFilter countFilter = new SingleColumnValueFilter(CF_WORD, COUNT,
						CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(minCount));
				FilterList filterList = new FilterList();
				filterList.addFilter(userFilter);
				filterList.addFilter(countFilter);
				Scan scan = new Scan();
				scan.setFilter(filterList);
				scan.addColumn(CF_WORD, USERID);
				scan.addColumn(CF_WORD, VALUE);
				scan.addColumn(CF_WORD, COUNT);
				scan.setCaching(500);

				try (ResultScanner scanner = hTable.getScanner(scan)) {
					for (Result result = scanner.next(); result != null; result = scanner.next()) {
						byte[] word = result.getValue(CF_WORD, VALUE);
						byte[] count = result.getValue(CF_WORD, COUNT);
						words.put(Bytes.toString(word), Bytes.toInt(count));
					}
				}
			}
		}

		return sortByValueDesc(words);
	}

	private void saveFeed(Connection connection, User profile, List<Post> feed) throws IOException {
		try (HTable hTable = (HTable) connection.getTable(FEED_TABLE)) {
			List<Put> puts = new ArrayList<>(feed.size());
			for (Post post : feed) {
				Put put = new Put(Bytes.toBytes(post.getId()));
				put.addColumn(CF_POST, USERID, Bytes.toBytes(profile.getId()));
				put.addColumn(CF_POST, MESSAGE, Bytes.toBytes(post.getMessage()));
				puts.add(put);
			}
			hTable.put(puts);
		}
	}

	private void saveProfile(Connection connection, User profile) throws IOException {
		try (HTable hTable = (HTable) connection.getTable(PROFILE_TABLE)) {
			Put put = new Put(Bytes.toBytes(profile.getId()));
			put.addColumn(CF_USER, USERNAME, Bytes.toBytes(profile.getName()));
			put.addColumn(CF_USER, JOBSTATUS, PENDING);
			hTable.put(put);
		}
	}

	private Map<String, Integer> sortByValueDesc(Map<String, Integer> map) {
		return map.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x, y) -> {
					throw new AssertionError();
				} , LinkedHashMap::new));

	}

}