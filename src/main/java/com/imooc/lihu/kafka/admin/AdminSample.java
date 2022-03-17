package com.imooc.lihu.kafka.admin;

import org.apache.kafka.clients.admin.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

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
    public static void createTopic() throws InterruptedException {

        AdminClient adminClient = adminClient();
        // 副本因子
        short replicationFactor = 1;
        NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, replicationFactor);
        // Arrays.asList(newTopic)
        CreateTopicsResult topics = adminClient.createTopics(Collections.singletonList(newTopic));
        System.out.println("CreateTopicsResult:" + topics);
    }

    /**
     * 获取Topic列表
     */
    public static void topicLists() throws ExecutionException, InterruptedException {

        AdminClient adminClient = adminClient();
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> names = listTopicsResult.names().get();
        // 打印names
        names.stream().forEach(System.out::println);
    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {

//        System.out.println("adminClient:" + AdminSample.adminClient());

//        createTopic();

        topicLists();
    }
}
