package social;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.stereotype.Component;

@Component
public class HBaseInit implements CommandLineRunner {

	@Autowired
	private HbaseTemplate hbaseConfig;

	@Override
	public void run(String... args) throws Exception {

		boolean force = args.length > 0 && args[0].equals("force");

		try (Connection connection = ConnectionFactory.createConnection(hbaseConfig.getConfiguration())) {

			// Instantiating HbaseAdmin class
			HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

			createTable(admin, HBaseService.FEED_TABLE, force, HBaseService.CF_POST);
			createTable(admin, HBaseService.PROFILE_TABLE, force, HBaseService.CF_USER);
			createTable(admin, HBaseService.WORDCOUNT_TABLE, force, HBaseService.CF_WORD);
		}
	}

	private void createTable(HBaseAdmin admin, TableName tableName, boolean force, byte[]... columnFamiles)
			throws IOException {

		boolean exists = admin.tableExists(tableName);

		if (!exists || force) {

			if (exists) {
				if (!admin.isTableDisabled(tableName)) {
					admin.disableTable(tableName);
				}
				admin.deleteTable(tableName);
			}

			// Instantiating table descriptor class
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

			// Adding column families to table descriptor
			for (byte[] family : columnFamiles) {
				tableDescriptor.addFamily(new HColumnDescriptor(family));
			}

			// Execute the table through admin
			admin.createTable(tableDescriptor);
		}
	}

}