package com.edgewalk.rabbitmq.boot.simple;

import com.edgewalk.rabbitmq.boot.raw.SendRawMsg;
import com.edgewalk.rabbitmq.boot.raw.SpringBootRabbitRawApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Created by huangrongyou@yixin.im on 2018/2/27.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes= SpringBootRabbitRawApplication.class, value = "spring.profiles.active=boot")
public class RawTest {

    @Autowired
    private SendRawMsg sendRawMsg;

    @Test
    public void sendAndReceive_2() throws Exception {
        String testContent = "send msg via spring boot -2";
        sendRawMsg.sendRaw(testContent);
        try {
            Thread.sleep(1000 * 10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
