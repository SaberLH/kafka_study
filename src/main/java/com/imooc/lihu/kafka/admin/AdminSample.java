package com.imooc.lihu.kafka.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 * @author lihu
 * @Date 2022-03-17 0:07
 */
public class AdminSample {

    public final static String TOPIC_NAME = "jz_topic";

    /**
     * 设置AdminClient
     */
    public static AdminClient adminClient(){

        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.38.129:9092");
        return AdminClient.create(properties);
    }

    /**
     * 创建topic实例
     */
    public static void createTopic(){

        AdminClient adminClient = adminClient();
        // 副本因子
        short replicationFactor = 1;
        NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, replicationFactor);
        // Arrays.asList(newTopic)
        CreateTopicsResult topics = adminClient.createTopics(Collections.singletonList(newTopic));
        System.out.println("CreateTopicsResult:" + topics);
    }


    public static void main(String[] args) {

//        System.out.println("adminClient:" + AdminSample.adminClient());

        createTopic();
    }
}
