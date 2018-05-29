package com.edgewalk.rabbitmq;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by huangrongyou@yixin.im on 2018/1/29.
 */
public class BaseTest {
    // 测试线程池
    protected ExecutorService executorService = Executors.newFixedThreadPool(10);

    // rabbitmq的IP地址
    protected final String rabbitmq_host = "140.143.224.21";
    // rabbitmq的web控制台用户名称
    protected final String rabbitmq_user = "guest";
    // rabbitmq的web控制台用户密码
    protected final String rabbitmq_pwd = "guest";
}
