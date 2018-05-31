package com.edgewalk.rabbitmq.basic;

import com.edgewalk.rabbitmq.BaseTest;
import com.edgewalk.rabbitmq.basic.direct.DirectRecv;
import com.edgewalk.rabbitmq.basic.direct.DirectSend;
import com.edgewalk.rabbitmq.basic.fonout.Publish;
import com.edgewalk.rabbitmq.basic.fonout.Subscribe;
import com.edgewalk.rabbitmq.basic.header.HeaderRecv;
import com.edgewalk.rabbitmq.basic.header.HeaderSend;
import com.edgewalk.rabbitmq.basic.rpc.RpcClient;
import com.edgewalk.rabbitmq.basic.rpc.RpcServer;
import com.edgewalk.rabbitmq.basic.topics.TopicsRecv;
import com.edgewalk.rabbitmq.basic.topics.TopicsSend;
import com.edgewalk.rabbitmq.basic.workqueues.WorkQueuesRecv;
import com.edgewalk.rabbitmq.basic.workqueues.WorkQueuesSend;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;


public class BasicTest extends BaseTest {

	@Test
	public void workqueues() throws InterruptedException {
		// 接收端
		int recNum = 2;
		while (recNum-- > 0) {
			final int recId = recNum;
			executorService.submit(() -> {
				WorkQueuesRecv.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, recId);
			});
		}
		Thread.sleep(5 * 100);
		// 发送端
		int sendNum = 4;
		while (sendNum-- > 0) {
			executorService.submit(() -> {
				WorkQueuesSend.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd);
			});
		}

		// sleep 10s
		Thread.sleep(10 * 1000);
	}

	@Test
	public void publishsubscribe() throws InterruptedException {
		// 接收端
		int recNum = 2;
		while (recNum-- > 0) {
			final int recId = recNum;
			executorService.submit(() -> {
				Subscribe.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, recId);
			});
		}
		Thread.sleep(5 * 100);
		// 发送端
		int sendNum = 2;
		while (sendNum-- > 0) {
			final int recId = recNum;
			executorService.submit(() -> {
				Publish.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, recId);
			});
		}

		// sleep 10s
		Thread.sleep(10 * 1000);
	}

	@Test
	public void direct_1() throws InterruptedException {
		// 接收端 1：绑定 orange 值
		executorService.submit(() -> {
			String[] routingKeys = {"orange"};
			DirectRecv.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, routingKeys);
		});
		// 接收端 2：绑定 black、green 值
		executorService.submit(() -> {
			String[] routingKeys = {"black", "green"};
			DirectRecv.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, routingKeys);
		});

		Thread.sleep(5 * 100);
		// 发送端 ： 发送 black，只有接收端1收到
		executorService.submit(() -> {
			String routingKey = "orange";
			DirectSend.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, routingKey);
		});

		// 发送端 ： 发送 green、black，只有接收端2收到
		executorService.submit(() -> {
			String routingKey = "green";
			DirectSend.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, routingKey);
		});

		// sleep 10s
		Thread.sleep(10 * 1000);
	}

	@Test
	public void direct_2() throws InterruptedException {

		// 接收端：同时创建两个接收端，同时绑定black
		int recNum = 2;
		while (recNum-- > 0) {
			// 接收端：绑定 black 值
			executorService.submit(() -> {
				String[] routingKeys = {"black"};
				DirectRecv.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, routingKeys);
			});
		}

		Thread.sleep(5 * 100);
		// 发送端1 ： 发送 black，所有的接收端都会收到
		executorService.submit(() -> {
			String routingKey = "black";
			DirectSend.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, routingKey);
		});

		// 发送端2 ： 发送 green，所有的接收端都不会收到
		executorService.submit(() -> {
			String routingKey = "green";
			DirectSend.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, routingKey);
		});

		// sleep 10s
		Thread.sleep(10 * 1000);
	}

	@Test
	public void topics() throws InterruptedException {

		// 消费者1：绑定 *.orange.* 值
		executorService.submit(() -> {
			String[] bindingKeys = {"*.orange.*"};
			TopicsRecv.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, bindingKeys);
		});

		// 消费者2：绑定  "*.*.simple2" 和 "lazy.#"值
		executorService.submit(() -> {
			String[] bindingKeys = {"*.*.simple2", "lazy.#"};
			TopicsRecv.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, bindingKeys);
		});

		Thread.sleep(5 * 100);
		// 生产者1 ： 发送 black，所有的接收端都会收到
		executorService.submit(() -> {
			String routingKey = "quick.orange.simple2";
			TopicsSend.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, routingKey);
		});

		// 生产者2 ： 发送 green，所有的接收端都不会收到
		executorService.submit(() -> {
			String routingKey = "lazy.pink.simple2";
			TopicsSend.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, routingKey);
		});

		// sleep 10s
		Thread.sleep(10 * 1000);
	}

	@Test
	public void rpc() throws InterruptedException {

		// rpc服务端
		executorService.submit(() -> {
			RpcServer.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd);
		});

		// rpc客户端
		executorService.submit(() -> {
			RpcClient.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, "I'am RpcClient");
		});

		// sleep 10s
		Thread.sleep(10 * 1000);
	}

	@Test
	public void header() throws InterruptedException {

		// 消费者1：绑定 format=pdf,type=report
		executorService.submit(() -> {
			Map<String, Object> headers = new HashMap();
			headers.put("format", "pdf");
			headers.put("type", "report");
			headers.put("x-match", "all");
			HeaderRecv.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, headers);
		});

		// 消费者2：绑定  format=pdf,type=log
		executorService.submit(() -> {
			Map<String, Object> headers = new HashMap();
			headers.put("format", "pdf");
			headers.put("type", "log");
			headers.put("x-match", "any");
			HeaderRecv.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, headers);
		});

		// 消费者3：绑定  format=zip,type=report
		executorService.submit(() -> {
			Map<String, Object> headers = new HashMap();
			headers.put("format", "zip");
			headers.put("type", "report");
			headers.put("x-match", "all");
			//   headers.put("x-match","any");
			HeaderRecv.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, headers);
		});

		Thread.sleep(2 * 1000);
		System.out.println("=============消息1===================");
		// 生产者1 ： format=pdf,type=reprot,x-match=all
		executorService.submit(() -> {
			Map<String, Object> headers = new HashMap();
			headers.put("format", "pdf");
			headers.put("type", "report");
			//     headers.put("x-match","all");
			HeaderSend.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, headers);
		});

		Thread.sleep(5 * 100);
		System.out.println("=============消息2===================");
		// 生产者2 ： format=pdf,x-match=any
		executorService.submit(() -> {
			Map<String, Object> headers = new HashMap();
			headers.put("format", "pdf");
			//     headers.put("x-match","any");
			HeaderSend.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, headers);
		});

		Thread.sleep(5 * 100);
		System.out.println("=============消息3===================");
		// 生产者1 ： format=zip,type=log,x-match=all
		executorService.submit(() -> {
			Map<String, Object> headers = new HashMap();
			headers.put("format", "zip");
			headers.put("type", "log");
			//      headers.put("x-match","all");
			HeaderSend.execute(rabbitmq_host, rabbitmq_user, rabbitmq_pwd, headers);
		});

		// sleep 10s
		Thread.sleep(10 * 1000);
	}

}
