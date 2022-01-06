package com.bellomhd.kafka.consumer;

import lombok.Data;

import java.io.Serializable;

@Data
public class MessageVo implements Serializable {

    private static final long serialVersionUID = 3441464870697052776L;

    private Long id;
    private String message;
}
