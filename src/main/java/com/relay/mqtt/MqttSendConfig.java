package com.relay.mqtt;

import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.outbound.MqttPahoMessageHandler;
import org.springframework.integration.mqtt.support.DefaultPahoMessageConverter;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

import java.util.Objects;

@Configuration
@IntegrationComponentScan
public class MqttSendConfig {

    private final MqttGateway mqttGateway;

    @Value("${spring.mqtt.username}")
    private String username;

    @Value("${spring.mqtt.password}")
    private String password;

    @Value("${spring.mqtt.url}")
    private String hostURL;

    @Value("${spring.mqtt.client.id}")
    private String clientId;

    @Value("${spring.mqtt.topic.app.subscriptions}")
    private String appSub;

    @Value("${spring.mqtt.topic.app.publish}")
    private String appPub;

    @Value("${spring.mqtt.topic.embedded.subscriptions}")
    private String embeddedSub;

    @Value("${spring.mqtt.topic.embedded.publish}")
    private String embeddedPub;

    @Value("${spring.mqtt.completionTimeout}")
    private int completionTimeout;

    @Autowired
    public MqttSendConfig(MqttGateway mqttGateway) {
        this.mqttGateway = mqttGateway;
    }

    //MQTT连接信息
    @Bean
    public MqttConnectOptions getMqttConnectOptions(){
        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setUserName(username);
        mqttConnectOptions.setPassword(password.toCharArray());
        mqttConnectOptions.setServerURIs(new String[]{hostURL});
        mqttConnectOptions.setKeepAliveInterval(2);
        return mqttConnectOptions;
    }

    //MQTT客户端
    @Bean
    public MqttPahoClientFactory mqttPahoClientFactory(){
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
        factory.setConnectionOptions(getMqttConnectOptions());
        return factory;
    }


    //MQTT信息通道（生产者）
    @Bean
    public MessageChannel mqttOutboundChannel(){
        return new DirectChannel();
    }

    //MQTT消息处理器（生产者）
    @Bean
    @ServiceActivator(inputChannel = "mqttOutboundChannel")
    public MessageHandler mqttOutBound(){
        MqttPahoMessageHandler messageHandler = new MqttPahoMessageHandler(clientId,mqttPahoClientFactory());
//        System.out.println("messageOut: Id"+clientId+" "+defaultTopic);
        messageHandler.setAsync(true);
//        messageHandler.setDefaultTopic(defaultTopic);
        return messageHandler;
    }


    //MQTT消息订阅绑定（消费者）
    @Bean
    public MessageProducer inbound(){
        //同时消费（订阅）多个topic
        MqttPahoMessageDrivenChannelAdapter adapter =
                new MqttPahoMessageDrivenChannelAdapter(
                        clientId+"_inbound",mqttPahoClientFactory(),appPub,embeddedPub);
        adapter.setCompletionTimeout(completionTimeout);
        adapter.setConverter(new DefaultPahoMessageConverter());
        adapter.setQos(1);
        //设置订阅通道
        adapter.setOutputChannel(mqttInboundChannel());
        return adapter;
    }

    //MQTT信息通道（消费者）
    @Bean
    public MessageChannel mqttInboundChannel(){
        return new DirectChannel();
    }

    //MQTT消息处理器（消费者）
    @Bean
    @ServiceActivator(inputChannel = "mqttInboundChannel")
    public MessageHandler handler(){
        return message -> {
            String payload = message.getPayload().toString();
            System.out.println(message.getHeaders());
            String topic = Objects.requireNonNull(message.getHeaders().get("mqtt_receivedTopic")).toString();
//            String type = topic.substring(topic.lastIndexOf("/")+1);
//            System.out.println("receive:"+payload);
            if(appPub.equalsIgnoreCase(topic)){
                System.out.println("------------App(mobileSub)------------");
                System.out.println("receive message from app: "+message);
                System.out.println("[topic]: "+topic+" [payload]: "+payload);
                System.out.println("relay payload to "+ embeddedSub);
                mqttGateway.sendToMqtt(embeddedSub,payload);
                System.out.println("----------------------------------------");
            }else if (embeddedPub.equalsIgnoreCase(topic)){
                System.out.println("------------Embedded(embeddedSub)------------");
                System.out.println("receive message from embedded: "+message);
                System.out.println("[topic]: "+topic+" [payload]: "+payload);
                System.out.println("relay payload to "+appSub);
                mqttGateway.sendToMqtt(appSub,payload);
                System.out.println("----------------------------------------");
            }else {
                System.out.println("-------------(Throw away)------------");
                System.out.println("topic: "+topic+ " payload: "+payload);
                System.out.println("----------------------------------------");
            }
        };
    }



}
