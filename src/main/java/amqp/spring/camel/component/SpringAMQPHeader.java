/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */

package amqp.spring.camel.component;

import java.util.HashMap;
import java.util.Map;
import org.springframework.amqp.core.Message;

public class SpringAMQPHeader {
    // The (settable) AMQP Basic Properties
    public static final String CONTENT_TYPE = "contentType";
    public static final String CONTENT_ENCODING = "contentEncoding";
    public static final String PRIORITY = "priority";
    public static final String CORRELATION_ID = "correlationId";
    public static final String REPLY_TO = "replyTo";
    public static final String EXPIRATION = "expiration";
    public static final String TYPE = "type";
    
    public static Message setBasicPropertiesFromHeaders(Message msg, Map<String, Object> headers) {
        for(Map.Entry<String, Object> headerEntry : headers.entrySet()) {
            String headerKey = headerEntry.getKey();
            Object headerValue = headerEntry.getValue();
            
            //Not switching on a string since we want to support Java >= 1.6
            if(CONTENT_ENCODING.equals(headerKey)) {
                msg.getMessageProperties().setContentEncoding(headerValue.toString());
            } else if(CONTENT_TYPE.equals(headerKey)) {
                msg.getMessageProperties().setContentEncoding(headerValue.toString());
            } else if(CORRELATION_ID.equals(headerKey)) {
                msg.getMessageProperties().setCorrelationId(headerValue.toString().getBytes());
            } else if(EXPIRATION.equals(headerKey)) {
                msg.getMessageProperties().setExpiration(headerValue.toString());
            } else if(PRIORITY.equals(headerKey)) {
                msg.getMessageProperties().setPriority(Integer.parseInt(headerValue.toString()));
            } else if(REPLY_TO.equals(headerKey)) {
                msg.getMessageProperties().setReplyTo(headerValue.toString());
            } else if(TYPE.equals(headerKey)) {
                msg.getMessageProperties().setType(headerValue.toString());
            }
        }
        
        return msg;
    }
    
    public static SpringAMQPMessage setBasicPropertiesToHeaders(SpringAMQPMessage msg, Message amqpMessage) {
        byte[] correlationId = amqpMessage.getMessageProperties().getCorrelationId();
        msg.getHeaders().put(CORRELATION_ID, correlationId == null ? null : new String(correlationId));
        msg.getHeaders().put(CONTENT_ENCODING, amqpMessage.getMessageProperties().getContentEncoding());
        msg.getHeaders().put(CONTENT_TYPE, amqpMessage.getMessageProperties().getContentType());
        msg.getHeaders().put(EXPIRATION, amqpMessage.getMessageProperties().getExpiration());
        msg.getHeaders().put(PRIORITY, amqpMessage.getMessageProperties().getPriority());
        msg.getHeaders().put(REPLY_TO, amqpMessage.getMessageProperties().getReplyTo());
        msg.getHeaders().put(TYPE, amqpMessage.getMessageProperties().getType());

        return msg;
    }
    
    public static Message copyHeaders(Message msg, Map<String, Object> headers) {
        for(Map.Entry<String, Object> headerEntry : headers.entrySet()) {
            if(! msg.getMessageProperties().getHeaders().containsKey(headerEntry.getKey())) {
                msg.getMessageProperties().setHeader(headerEntry.getKey(), headerEntry.getValue());
            }
        }
        
        return msg;
    }
    
    public static SpringAMQPMessage copyHeaders(SpringAMQPMessage msg, Map<String, Object> headers) {
        for(Map.Entry<String, Object> headerEntry : headers.entrySet()) {
            if(! SpringAMQPMessage.EXCHANGE_PATTERN.equals(headerEntry.getKey())) {
                msg.setHeader(headerEntry.getKey(), headerEntry.getValue());
            }
        }
        
        return msg;
    }
}
