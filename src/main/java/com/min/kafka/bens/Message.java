package com.min.kafka.bens;

import lombok.Data;

import java.util.Date;

/**
 * @program: kafka
 * @description:
 * @author: mcy
 * @create: 2018-08-03 17:51
 **/

@Data
public class Message {

    private Long id;    //id

    private String msg; //消息

    private Date sendTime;  //时间戳
}
