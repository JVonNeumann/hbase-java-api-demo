package com.github.hbasejavaapi.utils;

import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

public class HBasePrintUtil {

	public static void printResultScanner(ResultScanner resultScann) {
		for (Result result : resultScann) {
			printResult(result);
		}
	}

	public static void printResult(Result result) {
		List<Cell> cells = result.listCells();
		if (cells == null) {
			return;
		}
		for (Cell cell : cells) {
			printCell(cell);
		}
	}

	public static void printCell(Cell cell) {
		System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + "\t" + Bytes.toString(CellUtil.cloneFamily(cell))
				+ "\t" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" + cell.getTimestamp() + "\t"
				+ Bytes.toString(CellUtil.cloneValue(cell)));
	}

	public static void printTableDescriptor(TableDescriptor desc) {
		System.out.println("Name：" + desc.getTableName() + "\n" + "FlushPolicyClassName："
				+ desc.getFlushPolicyClassName() + "\n" + "MaxFileSize：" + desc.getMaxFileSize() + "\n"
				+ "MemStoreFlushSize：" + desc.getMemStoreFlushSize() + "\n" + "RegionReplication："
				+ desc.getRegionReplication() + "\n" + "RegionSplitPolicyClassName："
				+ desc.getRegionSplitPolicyClassName() + "\n" + "---------------------------------------------");
	}

	public static void printHColumnDescriptors(ColumnFamilyDescriptor[] columnDescs) {
		for (ColumnFamilyDescriptor desc : columnDescs) {
			printHColumnDescriptor(desc);
		}
	}

	public static void printHColumnDescriptor(ColumnFamilyDescriptor desc) {
		System.out.println("Name : " + desc.getNameAsString() + "\n" + "BloomFilterType : " + desc.getBloomFilterType()
				+ "\n" + "MinVersions : " + desc.getMinVersions() + "\n" + "MaxVersions : " + desc.getMaxVersions()
				+ "\n" + "InMemory : " + desc.isInMemory() + "\n" + "DataBlockEncoding : " + desc.getDataBlockEncoding()
				+ "\n" + "TimeToLive : " + desc.getTimeToLive() + "\n" + "Compression : "
				+ desc.getCompactionCompressionType() + "\n" + "BlockCacheEnabled : " + desc.isBlockCacheEnabled()
				+ "\n" + "Blocksize : " + desc.getBlocksize() + "\n" + "---------------------------------------------");
	}
}
