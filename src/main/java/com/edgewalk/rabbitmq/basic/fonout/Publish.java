package com.edgewalk.rabbitmq.basic.fonout;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Publish {


	private static final String EXCHANGE_NAME = "logs";

	public static void execute(String host, String userName, String password, int id) {
		// 配置连接工厂
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(host);
		// 使用管理后台默认guest帐号
		factory.setUsername(userName);
		factory.setPassword(password);

		try (//自动关流
				// 建立TCP连接
				Connection connection = factory.newConnection();
				// 在TCP连接的基础上创建通道
				Channel channel = connection.createChannel();
		) {
			// 声明一个fanout交换机
			// channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
			channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT, false, false, null);
			String message = "Publish-" + System.nanoTime();
			// 发送消息
			channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
			System.out.println(" [Publish" + id + "] Sent '" + message + "'");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
