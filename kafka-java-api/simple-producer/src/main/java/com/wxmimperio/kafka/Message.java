package com.wxmimperio.kafka;

/**
 * Created by wxmimperio on 2017/6/4.
 */
public class Message {

    private String topic;
    private String message;
    private String eventTime;

    @Override
    public String toString() {
        return "Message{" +
                "topic='" + topic + '\'' +
                ", message='" + message + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }
}
