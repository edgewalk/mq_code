package com.edgewalk.rabbitmq.basic.direct;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class DirectSend {
	private static final String EXCHANGE_NAME = "direct_logs";

	public static void execute(String host, String userName, String password, String routingKey) {
		// 配置连接工厂
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(host);
		// 使用管理后台默认guest帐号
		factory.setUsername(userName);
		factory.setPassword(password);

		try (
				// 建立TCP连接
				Connection connection = factory.newConnection();
				// 在TCP连接的基础上创建通道
				Channel channel = connection.createChannel();
		) {
			// 声明一个direct交换机
			channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
			String message = "DirectSend-" + System.currentTimeMillis();
			// 发送消息，并配置消息的路由键
			channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
			System.out.println(" [DirectSend] Sent '" + routingKey + "':'" + message + "'");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

