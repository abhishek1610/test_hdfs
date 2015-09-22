/**
 * Created by abhishek on 2015-08-29.
 */

import java.io.IOException ;
        import org.apache.hadoop.conf.Configuration ;
        import org.apache.hadoop.fs.FileSystem;
        import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

public class Hmkdirsfromhdfs {

    public static void main(String[] args)

            throws IOException {
        Configuration conf = new Configuration();
        conf.addResource("/etc/hadoop/conf/core-site.xml");
        conf.addResource("/etc/hadoop/conf/hdfs-site.xml");
        conf.set("fs.defaultFS", "hdfs://192.168.60.131:8020/");
        conf.set("hadoop.job.ugi", "cloudera");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        FileSystem fs = FileSystem.get(conf);
        boolean success = fs.mkdirs(new Path("/user/cloudera/testdirectory9"));
        System.out.println(success);

        Path inFile = new Path("/user/clouder/sample.txt");
        Path outFile = new Path("/user/clouder/out.txt");

        if (fs.exists(outFile))
            System.out.println("Output already exists");

        FSDataInputStream in = fs.open(inFile);

        FSDataOutputStream out = fs.create(outFile);

   int bytesRead;
        Byte buffer;

        while ((bytesRead = in.read(buffer)) > 0) {
            out.write(buffer, 0, bytesRead);
        }



                in.close();
        out.close();
    }

}
