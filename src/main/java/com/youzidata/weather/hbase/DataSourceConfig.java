package com.youzidata.weather.hbase;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

import java.io.IOException;

@Configuration
public class DataSourceConfig {
    @Autowired
    private Environment env;

//    public static org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
//    public static Connection connection;

    @Bean
    public HbaseTemplate hbaseTemplate() {
    	org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.set("hbase.master", env.getProperty("app.datasource.hbase.master"));
        configuration.set("hbase.zookeeper.quorum", env.getProperty("app.datasource.hdp.zk-quorum"));
        configuration.set("hbase.zookeeper.property.clientPort", env.getProperty("app.datasource.hdp.zk-port"));
        configuration.set("zookeeper.znode.parent", env.getProperty("app.datasource.hdp.zookeeper.znode.parent"));
        configuration.set("hbase.client.pause", "30");
        configuration.set("hbase.client.retries.number", "3");
        configuration.set("hbase.rpc.timeout", "100000");
//        configuration.set("mapreduce.map.output.compress", "true");
//        configuration.set("mapreduce.map.output.compress.codec", "snappy");
        configuration.set("hfile.block.index.cacheonwrite", "true");

//        configuration.set("hbase.regionserver.handler.count", "50");
        configuration.set("hbase.rootdir", env.getProperty("app.datasource.hbase.rootdir"));
//        try {
//            connection = ConnectionFactory.createConnection(configuration);
//        } catch (IOException e) {
//            e.printStackTrace ();
//        }
        return new HbaseTemplate(configuration);
    }
}
