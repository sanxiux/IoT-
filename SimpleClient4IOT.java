/**
 * aliyun.com Inc.
 * Copyright (c) 2004-2017 All Rights Reserved.
 */
package com.aliyun.iot.demo.iothub;

import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.aliyun.iot.util.LogUtil;
import com.aliyun.iot.util.SignUtil;

/**
 * IoT套件JAVA版设备接入demo
 */
public class SimpleClient4IOT {
	
	/******这里是客户端需要的参数*******/
    public static String deviceName = "VtyboYCj3HT7L8xQ8EvB";
    public static String productKey = "a1lN00ulvie";
    public static String secret = "Ug9FxeFxyy8fPWQdrcnsqnzCC57J07uV";

    //用于测试的topic
    private static String subTopic = "/sys/" + productKey + "/" + deviceName + "/thing/service/property/set";
    private static String pubTopic = "/sys/" + productKey + "/" + deviceName + "/thing/event/property/post";

    public static void main(String... strings) throws Exception {
        //客户端设备自己的一个标记，建议是MAC或SN，不能为空，32字符内
        String clientId = InetAddress.getLocalHost().getHostAddress();

        //设备认证
        Map<String, String> params = new HashMap<String, String>();
        params.put("productKey", productKey); //这个是对应用户在控制台注册的 设备productkey
        params.put("deviceName", deviceName); //这个是对应用户在控制台注册的 设备name
        params.put("clientId", clientId);
        String t = System.currentTimeMillis() + "";
        params.put("timestamp", t);

        //MQTT服务器地址，TLS连接使用ssl开头
        String targetServer = "ssl://" + productKey + ".iot-as-mqtt.cn-shanghai.aliyuncs.com:1883";

        //客户端ID格式，两个||之间的内容为设备端自定义的标记，字符范围[0-9][a-z][A-Z]
        String mqttclientId = clientId + "|securemode=2,signmethod=hmacsha1,timestamp=" + t + "|";
        String mqttUsername = deviceName + "&" + productKey; //mqtt用户名格式
        String mqttPassword = SignUtil.sign(params, secret, "hmacsha1"); //签名

        System.err.println("mqttclientId=" + mqttclientId);

        connectMqtt(targetServer, mqttclientId, mqttUsername, mqttPassword, deviceName);
    }

    public static void connectMqtt(String url, String clientId, String mqttUsername,
                                   String mqttPassword, final String deviceName) throws Exception {
        MemoryPersistence persistence = new MemoryPersistence();
        SSLSocketFactory socketFactory = createSSLSocket();
        final MqttClient sampleClient = new MqttClient(url, clientId, persistence);
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setMqttVersion(4); // MQTT 3.1.1
        connOpts.setSocketFactory(socketFactory);

        //设置是否自动重连
        connOpts.setAutomaticReconnect(true);

        //如果是true，那么清理所有离线消息，即QoS1或者2的所有未接收内容
        connOpts.setCleanSession(false);

        connOpts.setUserName(mqttUsername);
        connOpts.setPassword(mqttPassword.toCharArray());
        connOpts.setKeepAliveInterval(65);

        LogUtil.print(clientId + "进行连接, 目的地: " + url);
        sampleClient.connect(connOpts);

        sampleClient.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                LogUtil.print("连接失败,原因:" + cause);
                cause.printStackTrace();
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                LogUtil.print("接收到消息,来至Topic [" + topic + "] , 内容是:["
                    + new String(message.getPayload(), "UTF-8") + "],  ");
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                //如果是QoS0的消息，token.resp是没有回复的
                LogUtil.print("消息发送成功! " + ((token == null || token.getResponse() == null) ? "null"
                    : token.getResponse().getKey()));
            }
        });
        LogUtil.print("连接成功:---");
        String string  =  String.valueOf(new Date().getTime());
        //这里测试发送一条消息
                String content = "{\"id\": \""+string+"\",\"params\": {\"IndoorTemperature\":20,\"EnvironmentHumidity\": 40},\"method\": \"thing.event.property.post\",\"version\":\"1.0.0\"}";
//        String content = "{\"params\":{\"method\":\"thing.service.property.set\",\"id\":\"165344211\",\"params\":{\"EnvironmentHumidity\":10},\"version\":\"1.0.0\"},\"code\":\"200\",\"gmtCreate\":\"1541323802\",\"method\":\"/sys/a1lN00ulvie/VtyboYCj3HT7L8xQ8EvB/thing/service/property/set\",\"type\":\"downstream\"}";
 

        MqttMessage message = new MqttMessage(content.getBytes("utf-8"));
        message.setQos(0);
        //System.out.println(System.currentTimeMillis() + "消息发布:---");
        sampleClient.publish(pubTopic, message);

        //一次订阅永久生效 
        //这个是第一种订阅topic方式，回调到统一的callback
        sampleClient.subscribe(subTopic);

        //这个是第二种订阅方式, 订阅某个topic，有独立的callback
        //sampleClient.subscribe(subTopic, new IMqttMessageListener() {
        //    @Override
        //    public void messageArrived(String topic, MqttMessage message) throws Exception {
        //
        //        LogUtil.print("收到消息：" + message + ",topic=" + topic);
        //    }
        //});

        //回复RRPC响应
        final ExecutorService executorService = new ThreadPoolExecutor(2,
            4, 600, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(100), new CallerRunsPolicy());

        String reqTopic = "/sys/" + productKey + "/" + deviceName + "/rrpc/request/+";
        sampleClient.subscribe(reqTopic, new IMqttMessageListener() {
            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                LogUtil.print("收到请求：" + message + ", topic=" + topic);
                String messageId = topic.substring(topic.lastIndexOf('/') + 1);
                final String respTopic = "/sys/" + productKey + "/" + deviceName + "/rrpc/response/" + messageId;
                String content = "hello world";
                final MqttMessage response = new MqttMessage(content.getBytes());
                response.setQos(0); //RRPC只支持QoS0
                //不能在回调线程中调用publish，会阻塞线程，所以使用线程池
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            sampleClient.publish(respTopic, response);
                            LogUtil.print("回复响应成功，topic=" + respTopic);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        });
    }

    private static SSLSocketFactory createSSLSocket() throws Exception {
        SSLContext context = SSLContext.getInstance("TLSV1.2");
        context.init(null, new TrustManager[] {new ALiyunIotX509TrustManager()}, null);
        SSLSocketFactory socketFactory = context.getSocketFactory();
        return socketFactory;
    }
}
