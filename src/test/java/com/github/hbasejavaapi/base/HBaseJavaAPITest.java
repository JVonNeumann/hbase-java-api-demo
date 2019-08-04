package com.github.hbasejavaapi.base;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.Test;

import com.github.hbasejavaapi.utils.HBasePrintUtil;
import com.github.hbasejavaapi.utils.HBaseUtil;

/**
 * 在HBase中建立两张表：
 * 
 * 用户表：USER,
 * 该表含有两个列族：一个为用户属性列族（族名：userProperty）；另外一个为用户行为属性（actionProperty）。RowKey为:用户ip
 * 
 * 广告表：ADVERT, 其含有一个列族（adProperty）
 * 
 * 三个列族： USER：userProperty：用户Ip区域（ipArea），用户ip国际(ipCountry)，用户名ID(userId)
 * 
 * USER：actionProperty：点击链接(link)；广告RowKey（adId）；点击时间（time）,
 * 
 * ADERT：adProperty：广告所属行业（adHy）,广告利润（adMoney），广告所属商家comp，
 * 
 * refer to: https://my.oschina.net/jeeker/blog/634789
 */
public class HBaseJavaAPITest {

	@Test
	public void createTable() throws Exception {
		HBaseUtil.createTable("USER", new String[] { "userProperty", "actionProperty" });
		HBaseUtil.createTable("ADVERT", new String[] { "adProperty", "adPrice", "adTime" });
	}

	@Test
	public void modifyTable() throws Exception {
		HBaseUtil.modifyTable("USER", new String[] { "cf1", "cf2" }, new String[] { "actionProperty" });
	}

	@Test
	public void getAllTables() throws Exception {
		HBaseUtil.getAllTables();
	}

	@Test
	public void descTable() throws Exception {
		HBaseUtil.descTable("USER");
		HBaseUtil.descTable("ADVERT");
	}
	
	@Test
	public void dropTable() throws Exception {
		HBaseUtil.dropTable("USER");
		HBaseUtil.dropTable("ADVERT");
	}

	@Test
	public void putData() throws Exception {
		// 保存广告数据, 没有指定column的名字
		HBaseUtil.putData("ADVERT", "ad00120311", "adProperty", "", "汽车");
		HBaseUtil.putData("ADVERT", "ad00120311", "adProperty", "", "信息技术");

		HBaseUtil.putData("ADVERT", "ad00120312", "adPrice", "", "$500/day");
		HBaseUtil.putData("ADVERT", "ad00120312", "adPrice", "", "$800/day");

		HBaseUtil.putData("ADVERT", "ad00120312", "adTime", "", "24h");
		HBaseUtil.putData("ADVERT", "ad00120312", "adTime", "", "72h");

		// 保存广告数据, 指定column的名字
		HBaseUtil.putData("ADVERT", "ad00120313", "adProperty", "adHy", "家具");
		HBaseUtil.putData("ADVERT", "ad00120313", "adProperty", "adMoney", "11.11");
		HBaseUtil.putData("ADVERT", "ad00120313", "adProperty", "comp", "淘宝");

		// 保存用户属性, 指定column的名字
		HBaseUtil.putData("USER", "11.11.11.1", "userProperty", "ipArea", "上海");
		HBaseUtil.putData("USER", "11.11.11.1", "userProperty", "ipCountry", "中国");
		HBaseUtil.putData("USER", "11.11.11.1", "userProperty", "userId", "100001213");

		// 保存用户行为, 指定column的名字
		HBaseUtil.putData("USER", "11.11.11.1", "actionProperty", "link", "/ifeng.com/......");
		HBaseUtil.putData("USER", "11.11.11.1", "actionProperty", "adId", "ad00120311");
		HBaseUtil.putData("USER", "11.11.11.1", "actionProperty", "time", "20130623172900");
	}

	@Test
	public void getResult() throws Exception {
		// 根据RowKey读取数据一行数据
		Result result = HBaseUtil.getResult("ADVERT", "ad00120311");
		HBasePrintUtil.printResult(result);

		// 根据TableName读取数据全部数据
		ResultScanner scanner = HBaseUtil.getResultScann("USER");
		HBasePrintUtil.printResultScanner(scanner);
	}

	@Test
	public void deleteColumn() throws Exception {
		// 根据RowKey删除
		HBaseUtil.deleteColumn("ADVERT", "ad00120311");
		HBaseUtil.deleteColumn("USER", "11.11.11.1");

		// 根据RowKey+FamilyName删除
		HBaseUtil.deleteColumn("ADVERT", "ad00120312", "adPrice");

		// 根据RowKey+FamilyName+Column删除
		HBaseUtil.deleteColumn("ADVERT", "ad00120313", "adProperty", "adHy");
	}

}
