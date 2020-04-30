package com.microsoft.hdinsight;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.net.URI;

public class HDFSClient {
    public static void main(String[] args) throws  Exception{

        FileSystem fs = FileSystem.get(new URI("hdfs://10.160.254.62:8020"), new Configuration(), "hdfs");
        FileStatus[] statuses = fs.listStatus(new Path("/"));

        for(FileStatus status:statuses){
            if(status.isFile()){
                System.out.println("---fine---");
                System.out.println(status.getPath());
                System.out.println(status.getLen());
            }else{
                System.out.println("----folder info--------");
                System.out.println(status.getPath());
            }
        }
    }
}
