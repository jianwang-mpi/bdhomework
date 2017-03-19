import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * Created by wangjian on 17-3-19.
 */
public class Hw1Grp2 {
    public static void main(String args[]) {
        Hw1Grp2 hw1Grp2 = new Hw1Grp2();
        try {
            hw1Grp2.run(args);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void run(String[] args) throws IOException {
        String filePath = getFilePath(args[0]);
        int groutBy = getGroupBy(args[1]);
        Res res = getRes(args[2]);
        System.out.println(filePath + groutBy + res);

        List<String> dataLines = readFile(filePath);
        List<List<String>> data = getData(dataLines);
        for (String s : dataLines) {
            System.out.println(s);
        }
        if (res.count) {
            HashMap<String, Integer> countHashMap = new HashMap<String, Integer>();
            for (List<String> line : data) {
                String key = line.get(groutBy);
                if (countHashMap.containsKey(key)) {
                    countHashMap.put(key, countHashMap.get(key) + 1);
                } else {
                    countHashMap.put(key, 1);
                }
            }

        }

    }

    private void createDataBase() throws IOException {
        String tableName = "Result";
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        // create column descriptor
        HColumnDescriptor cf = new HColumnDescriptor("res");
        htd.addFamily(cf);
        // configure HBase
        Configuration configuration = HBaseConfiguration.create();
        HBaseAdmin hAdmin = new HBaseAdmin(configuration);
        hAdmin.createTable(htd);
        hAdmin.close();
    }

    private void saveHbase(HashMap<String, Object> source) {

    }

    private List<List<String>> getData(List<String> dataLines) {
        List<List<String>> result = new ArrayList<List<String>>(dataLines.size());
        for (String s : dataLines) {
            result.add(Arrays.asList(s.split("|")));
        }
        return result;
    }

    private List<String> readFile(String filePath) throws IOException {
        String file = "hdfs://localhost:9000" + filePath;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        FSDataInputStream in_stream = fs.open(path);
        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
        String s;
        List<String> result = new ArrayList<String>();
        while ((s = in.readLine()) != null) {
            result.add(s);
        }
        in.close();
        fs.close();
        return result;
    }

    private String getFilePath(String rawFilePath) {
        return String.valueOf(rawFilePath.toCharArray(), 2, rawFilePath.length() - 2);
    }

    private int getGroupBy(String rawGroupby) {
        return Integer.valueOf(String.valueOf(rawGroupby.toCharArray(), 9, rawGroupby.length() - 9));
    }


    private Res getRes(String rawRes) {
        String[] parameters = String.valueOf(rawRes.toCharArray(), 4, rawRes.length() - 4).split(",");
        Res res = new Res();
        for (String parameter : parameters) {
            if (parameter.startsWith("count")) {
                res.setCount(true);
            }
            if (parameter.startsWith("avg")) {
                res.setAvg(Integer.valueOf(String.valueOf(parameter.toCharArray(), 5, parameter.length() - 6)));
            }
            if (parameter.startsWith("max")) {
                res.setMax(Integer.valueOf(String.valueOf(parameter.toCharArray(), 5, parameter.length() - 6)));
            }
        }
        return res;
    }

    class Res {
        private boolean count;
        private Integer avg;
        private Integer max;

        public Res() {
            count = false;
            avg = null;
            max = null;
        }

        public boolean isCount() {
            return count;
        }

        public void setCount(boolean count) {
            this.count = count;
        }

        public Integer getAvg() {
            return avg;
        }

        public void setAvg(Integer avg) {
            this.avg = avg;
        }

        public Integer getMax() {
            return max;
        }

        public void setMax(Integer max) {
            this.max = max;
        }

        @Override
        public String toString() {
            return "Res{" +
                    "count=" + count +
                    ", avg=" + avg +
                    ", max=" + max +
                    '}';
        }
    }
}

