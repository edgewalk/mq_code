package com.edgewalk.rabbitmq.basic.rpc;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class RpcClient {
	private static final String RPC_QUEUE_NAME = "rpc_queue";

	public static void execute(String host, String userName, String password, String message) {
		// 配置连接工厂
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(host);
		// 需要在管理后台增加一个hry帐号
		factory.setUsername(userName);
		factory.setPassword(password);


		try (
				// 建立TCP连接
				Connection connection = factory.newConnection();
				// 在TCP连接的基础上创建通道
				Channel channel = connection.createChannel();
		) {

			// 定义临时队列，并返回生成的队列名称
			String callbackQueueName = channel.queueDeclare().getQueue();
			// 唯一标志本次请求
			String corrId = UUID.randomUUID().toString();

			// 生成发送消息的属性
			AMQP.BasicProperties props = new AMQP.BasicProperties
					.Builder()
					.correlationId(corrId) // 唯一标志本次请求
					.replyTo(callbackQueueName) // 设置回调队列
					.build();
			// 发送消息，发送到默认交换机
			channel.basicPublish("", RPC_QUEUE_NAME, props, message.getBytes("UTF-8"));
			System.out.println("["+LocalDate.now()+" "+LocalTime.now() +"] [RpcClient]-["+RPC_QUEUE_NAME+"] 发送消息: " + message+",correlationId="+corrId+",回调队列为:"+callbackQueueName );

			// 阻塞队列，用于存储回调结果
			final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);

			// 定义对replyQueueName队列收到消息的确认方法(自动确认)
			channel.basicConsume(callbackQueueName , true, new DefaultConsumer(channel) {
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
					if (properties.getCorrelationId().equals(corrId)) {
						response.offer(new String(body, "UTF-8"));
					}
				}
			});
			// 获取回调的结果
			String result = response.poll(5,TimeUnit.SECONDS);//等待五秒,获取不到则返回null
			System.out.println("["+LocalDate.now()+" "+LocalTime.now() +"] [RpcClient]-["+callbackQueueName +"] 收到消息: "+result);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
