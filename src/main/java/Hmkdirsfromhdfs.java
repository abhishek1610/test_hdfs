/**
 * Created by abhishek on 2015-08-29.
 */

import java.io.IOException ;
        import org.apache.hadoop.conf.Configuration ;
        import org.apache.hadoop.fs.FileSystem;
        import org.apache.hadoop.fs.Path;

public class Hmkdirsfromhdfs {

    public static void main(String[] args)

            throws IOException {
        Configuration conf = new Configuration();
        conf.addResource("/etc/hadoop/conf/core-site.xml");
        conf.addResource("/etc/hadoop/conf/hdfs-site.xml");
        conf.set("fs.defaultFS", "hdfs://192.168.60.128:8020/");
        conf.set("hadoop.job.ugi", "cloudera");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        FileSystem fs = FileSystem.get(conf);
        boolean success = fs.mkdirs(new Path("/user/cloudera/testdirectory9"));
        System.out.println(success);
    }

}
