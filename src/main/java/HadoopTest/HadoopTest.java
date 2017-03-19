package HadoopTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Created by wangjian on 17-3-18.
 */
public class HadoopTest {
    public static void main(String[] args) throws IOException {
        String dsf = "hdfs://localhost:9000/user/hadoop/1.txt";
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(dsf),conf);
        FSDataInputStream hdfsInStream = fs.open(new Path(dsf));

        byte[] ioBuffer = new byte[1024];
        int readLen = hdfsInStream.read(ioBuffer);
        while(readLen!=-1)
        {
            System.out.write(ioBuffer, 0, readLen);
            readLen = hdfsInStream.read(ioBuffer);
        }
        hdfsInStream.close();
        fs.close();
    }
}
