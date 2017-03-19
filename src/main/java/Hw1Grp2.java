import org.apache.commons.lang.SystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

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
        createDataBase();
        for (String s : dataLines) {
            System.out.println(s);
        }
        if (res.isCount()) {
            HashMap<String, Integer> countHashMap = new HashMap<String, Integer>();
            for (List<String> line : data) {
                String key = line.get(groutBy);
                if (countHashMap.containsKey(key)) {
                    countHashMap.put(key, countHashMap.get(key) + 1);
                } else {
                    countHashMap.put(key, 1);
                }
            }
            saveHbase(countHashMap, "count");
        }
        if (res.getAvg() != null) {
            HashMap<String, Integer> countHashMap = new HashMap<String, Integer>();
            HashMap<String, Double> averageHashMap = new HashMap<String, Double>();
            for (List<String> line : data) {
                String key = line.get(groutBy);
                if (countHashMap.containsKey(key)) {
                    countHashMap.put(key, countHashMap.get(key) + 1);
                    averageHashMap.put(key, averageHashMap.get(key) + Double.valueOf(line.get(res.getAvg())));
                } else {
                    countHashMap.put(key, 1);
                    averageHashMap.put(key, Double.valueOf(line.get(res.getAvg())));

                }
            }
            for (String key : countHashMap.keySet()) {
                averageHashMap.put(key, averageHashMap.get(key) / countHashMap.get(key));
            }
            saveHbase(averageHashMap, "avg(R" + res.getAvg() + ")");
        }
        if (res.getMax() != null) {
            HashMap<String, Comparable> compareHashMap = new HashMap<String, Comparable>();
            for (List<String> line : data) {
                String key = line.get(groutBy);
                Comparable value = parseValue(line.get(res.getMax()));
                if (compareHashMap.containsKey(key)) {
                    Comparable oldValue = compareHashMap.get(key);
                    if (oldValue.compareTo(value) < 0) {
                        compareHashMap.put(key, value);
                    }
                } else {
                    compareHashMap.put(key, value);
                }
            }
            saveHbase(compareHashMap, "max(R" + res.getMax() + ")");
        }

    }

    private Comparable parseValue(String stringValue) {
        Comparable result = null;
        if (Character.isDigit(stringValue.charAt(0))) {
            try {
                Integer integer = Integer.valueOf(stringValue);
                result = integer;
            } catch (Exception e) {
                Double d = Double.valueOf(stringValue);
                result = d;
            }
        } else {
            result = stringValue;
        }
        return result;
    }

    private void createDataBase() throws IOException {
        String tableName = "Result";
        // configure HBase
        Configuration configuration = HBaseConfiguration.create();
        HBaseAdmin hAdmin = new HBaseAdmin(configuration);
        if (hAdmin.tableExists(tableName)) {// if table to create exists, delete it first.
            hAdmin.disableTable(tableName);
            hAdmin.deleteTable(tableName);
            System.out.println(tableName + " is exist,detele....");
        }
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        htd.addFamily(new HColumnDescriptor("res"));
        hAdmin.createTable(htd);
        hAdmin.close();
    }

    private void saveHbase(HashMap<String, ?> source, String name) throws IOException {
        String tableName = "Result";
        Configuration configuration = HBaseConfiguration.create();
        HTable table = new HTable(configuration, tableName);
        for (String key : source.keySet()) {
            Put put = new Put(key.getBytes());
            put.add("res".getBytes(), name.getBytes(), source.get(key).toString().getBytes());
            table.put(put);
        }


        table.close();
        System.out.println("put successfully");
    }

    private List<List<String>> getData(List<String> dataLines) {
        List<List<String>> result = new ArrayList<List<String>>(dataLines.size());
        for (String s : dataLines) {
            result.add(Arrays.asList(s.split("[|]")));
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

