package com.github.hbasejavaapi.utils;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase基础的CRUD操作, 基于HBase3.0
 */
public class HBaseUtil {

	private static final String ZOOKEEPER_LIST = "host-131:2181,host-132:2181,host-133:2181";
	private static Configuration conf = HBaseConfiguration.create();
	private static Connection conn;
	private static HBaseAdmin admin;
	private static HTable table;

	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", ZOOKEEPER_LIST);
		try {
			conn = ConnectionFactory.createConnection(conf);
			admin = (HBaseAdmin) conn.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static HTable getTable(String tableName) throws Exception {
		return (HTable) conn.getTable(TableName.valueOf(tableName));
	}

	public static boolean isTableExist(String tableName) throws Exception {
		return admin.tableExists(TableName.valueOf(tableName));
	}

	public static void disableTable(String tableName) throws Exception {
		admin.disableTable(TableName.valueOf(tableName));
	}

	public static void enableTable(String tableName) throws Exception {
		admin.enableTable(TableName.valueOf(tableName));
	}

	public static boolean isTableDisabled(String tableName) throws Exception {
		return admin.isTableDisabled(TableName.valueOf(tableName));
	}

	public static boolean isTableEnabled(String tableName) throws Exception {
		return admin.isTableEnabled(TableName.valueOf(tableName));
	}

	public static void getAllTables() throws Exception {
		TableDescriptor[] tds = admin.listTables();
		for (TableDescriptor td : tds) {
			HBasePrintUtil.printTableDescriptor(td);
		}
	}

	public static void createTable(String tableName, String[] family) throws Exception {
		if (isTableExist(tableName)) {
			dropTable(tableName);
		}
		TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
		if (family != null && family.length > 0) {
			for (String cf : family) {
				ColumnFamilyDescriptor of = ColumnFamilyDescriptorBuilder.of(cf);
				tdb.setColumnFamily(of);
			}
		}
		TableDescriptor td = tdb.build();
		admin.createTable(td);
	}

	public static void descTable(String tableName) throws Exception {
		TableDescriptor td = admin.getTableDescriptor(TableName.valueOf(tableName));
		ColumnFamilyDescriptor[] cfd = td.getColumnFamilies();
		HBasePrintUtil.printHColumnDescriptors(cfd);
	}

	public static void dropTable(String tableName) throws Exception {
		if (isTableDisabled(tableName)) {
			admin.deleteTable(TableName.valueOf(tableName));
		} else {
			disableTable(tableName);
			admin.deleteTable(TableName.valueOf(tableName));
		}
	}

	public static void modifyTable(String tableName, String[] newFamily, String[] removeFamily) throws Exception {
		if (newFamily != null && newFamily.length > 0) {
			for (String cf : newFamily) {
				ColumnFamilyDescriptor of = ColumnFamilyDescriptorBuilder.of(cf);
				admin.addColumnFamily(TableName.valueOf(tableName), of);
			}

		}

		if (removeFamily != null && removeFamily.length > 0) {
			for (String cf : removeFamily) {
				admin.deleteColumnFamily(TableName.valueOf(tableName), cf.getBytes());
			}
		}

	}

	public static void putData(String tableName, String rowKey, String familyName, String columnName, String value)
			throws Exception {
		putData(tableName, rowKey, familyName, columnName, value, new Date().getTime());
	}

	public static void putData(String tableName, String rowKey, String familyName, String columnName, String value,
			long timestamp) throws Exception {
		table = getTable(tableName);
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), timestamp, Bytes.toBytes(value));
		table.put(put);
	}

	public static void putData(String tableName, Put put) throws Exception {
		table = getTable(tableName);
		table.put(put);
	}

	public static void putData(String tableName, List<Put> putList) throws Exception {
		table = getTable(tableName);
		table.put(putList);
	}

	public static Result getResult(String tableName, String rowKey) throws Exception {
		table = getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		return table.get(get);
	}

	public static Result getResult(String tableName, String rowKey, String familyName) throws Exception {
		table = getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addFamily(Bytes.toBytes(familyName));
		return table.get(get);
	}

	public static Result getResult(String tableName, String rowKey, String familyName, String columnName)
			throws Exception {
		table = getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
		return table.get(get);
	}

	public static Result getResultByVersion(String tableName, String rowKey, String familyName, String columnName,
			int versions) throws Exception {
		table = getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
		get.readVersions(versions);
		return table.get(get);
	}

	public static ResultScanner getResultScann(String tableName) throws Exception {
		table = getTable(tableName);
		Scan scan = new Scan();
		return table.getScanner(scan);
	}

	public static void deleteColumn(String tableName, String rowKey) throws Exception {
		table = getTable(tableName);
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		table.delete(delete);
	}

	public static void deleteColumn(String tableName, String rowKey, String falilyName) throws Exception {
		table = getTable(tableName);
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		delete.addFamily(Bytes.toBytes(falilyName));
		table.delete(delete);
	}

	public static void deleteColumn(String tableName, String rowKey, String falilyName, String columnName)
			throws Exception {
		table = getTable(tableName);
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		delete.addColumn(Bytes.toBytes(falilyName), Bytes.toBytes(columnName));
		table.delete(delete);
	}

}