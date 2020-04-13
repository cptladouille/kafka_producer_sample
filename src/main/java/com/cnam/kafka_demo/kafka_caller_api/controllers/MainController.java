package com.cnam.kafka_demo.kafka_caller_api.controllers;

import java.util.concurrent.atomic.AtomicLong;

import com.cnam.kafka_demo.kafka_caller_api.kafka.MyProducer;
import com.cnam.kafka_demo.kafka_caller_api.model.Message;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MainController {

    private final AtomicLong counter = new AtomicLong();
    MyProducer myProducer;

    MainController()
    {
        this.myProducer = new MyProducer("test");
    }


    @GetMapping("/api")
    public Message PostMessage(@RequestParam(value = "content", defaultValue = "messsage") String content) {
        myProducer.Send(content);
        return new Message(counter.incrementAndGet(), "Requete envoyée");
    }
}