package com.edgewalk.rabbitmq.basic.workqueues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;


public class WorkQueuesSend {
	private static final String TASK_QUEUE_NAME = "task_queue";

	public static void execute(String host, String userName, String password) {
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
			// queueDeclarePassive(String queue)可以用来检测一个queue是否已经存在
			// Queue.DeclareOk queueDeclarePassive(String queue) throws IOException
			// 声明一个队列
			channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

			String message = "Work Queues!" + System.currentTimeMillis();
			// 发送一个持久化消息
			channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes("UTF-8"));
			System.out.println(" [WorkQueuesSend] Sent '" + message + "'");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
