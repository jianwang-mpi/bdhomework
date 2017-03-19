package HbaseTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;

/**
 * Created by wangjian on 17-3-19.
 */
public class HbaseTest {
    public static void main(String args[]) throws IOException {
        String tableName= "mytable";
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
// create column descriptor
        HColumnDescriptor cf = new HColumnDescriptor("mycf");
        htd.addFamily(cf);
// configure HBase
        Configuration configuration = HBaseConfiguration.create();
        HBaseAdmin hAdmin = new HBaseAdmin(configuration);
        hAdmin.createTable(htd);
        hAdmin.close();
        HTable table = new HTable(configuration,tableName);
        Put put = new Put("abc".getBytes());
        put.add("mycf".getBytes(),"a".getBytes(),"789".getBytes());
        table.put(put);
        table.close();
        System.out.println("put successfully");
    }
}
