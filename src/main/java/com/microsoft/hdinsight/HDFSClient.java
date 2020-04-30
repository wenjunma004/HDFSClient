package com.microsoft.hdinsight;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;


import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

public class HDFSClient {
    private final Configuration conf = new Configuration();
    FileSystem fs;
    private UserGroupInformation ugi;
    public HDFSClient() throws IOException, InterruptedException {
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
       //fs = FileSystem.get(new URI("hdfs://localhost:8020"), new Configuration(), "hdfs");
        fs = FileSystem.get(conf);
        System.out.println("Home Directory:"+fs.getHomeDirectory());
        ugi = UserGroupInformation.getCurrentUser();

        fs = execute(new PrivilegedExceptionAction<FileSystem>() {
            public FileSystem run() throws IOException {
                System.out.println("run it...");
                return FileSystem.get(conf);
            }
        });


    }

    public void listFiles() throws IOException {
        FileStatus[] statuses = fs.listStatus(new Path("/"));
        for(FileStatus status:statuses){
            if(status.isFile()){
                System.out.println("---file---");
                System.out.println(status.getPath());
                System.out.println(status.getLen());
            }else{
                System.out.println("----folder info --------");
                System.out.println(status.getPath());
            }
        }


    }
    public static void main(String[] args) throws  Exception{
        HDFSClient hdfsClient = new HDFSClient();
        hdfsClient.listFiles();

    }

    /**
     * Executes action on HDFS using doAs
     * @param action strategy object
     * @param <T> result type
     * @return result of operation
     * @throws IOException
     * @throws InterruptedException
     */
    public <T> T execute(PrivilegedExceptionAction<T> action)
            throws IOException, InterruptedException {
        T result = null;

        // Retry strategy applied here due to HDFS-1058. HDFS can throw random
        // IOException about retrieving block from DN if concurrent read/write
        // on specific file is performed (see details on HDFS-1058).
        int tryNumber = 0;
        boolean succeeded = false;
        do {
            tryNumber += 1;
            try {
                result = ugi.doAs(action);
                succeeded = true;
            } catch (IOException ex) {
                if (!Strings.isNullOrEmpty(ex.getMessage()) && !ex.getMessage().contains("Cannot obtain block length for")) {
                    throw ex;
                }
                if (tryNumber >= 3) {
                    throw ex;
                }
                Thread.sleep(1000);  //retry after 1 second
            }
        } while (!succeeded);
        return result;
    }
}
