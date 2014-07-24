/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */

package amqp.spring.camel.component;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import java.util.Map;

public class SpringAMQPHeader {
    // The (settable) AMQP Basic Properties
    public static final String CONTENT_TYPE = "contentType";
    public static final String CONTENT_ENCODING = "contentEncoding";
    public static final String PRIORITY = "priority";
    public static final String MESSAGE_ID = "messageId";
    public static final String CORRELATION_ID = "correlationId";
    public static final String REPLY_TO = "replyTo";
    public static final String EXPIRATION = "expiration";
    public static final String DELIVERY_MODE = "deliveryMode";
    public static final String TYPE = "type";
    
    public static Message setBasicPropertiesFromHeaders(Message msg, Map<String, Object> headers) {
        for (Map.Entry<String, Object> headerEntry : headers.entrySet()) {
            String headerKey = headerEntry.getKey();
            Object headerValue = headerEntry.getValue();

            String headerValueString = null;
            if (headerValue != null) {
                headerValueString = headerValue.toString();
            }
            
            //Not switching on a string since we want to support Java >= 1.6
            if (CONTENT_ENCODING.equals(headerKey)) {
                msg.getMessageProperties().setContentEncoding(headerValueString);
            } else if(CONTENT_TYPE.equals(headerKey)) {
                msg.getMessageProperties().setContentType(headerValueString);
            } else if(MESSAGE_ID.equals(headerKey)) {
                msg.getMessageProperties().setMessageId(headerValueString);
            } else if(CORRELATION_ID.equals(headerKey)) {
                byte[] correlationId = headerValueString != null ? headerValueString.getBytes() : null;
                msg.getMessageProperties().setCorrelationId(correlationId);
            } else if(EXPIRATION.equals(headerKey)) {
                msg.getMessageProperties().setExpiration(headerValueString);
            } else if(PRIORITY.equals(headerKey)) {
                Integer priority = headerValueString != null ? Integer.parseInt(headerValueString) : null;
                msg.getMessageProperties().setPriority(priority);
            } else if(REPLY_TO.equals(headerKey)) {
                msg.getMessageProperties().setReplyTo(headerValueString);
            } else if(DELIVERY_MODE.equals(headerKey)) {
                msg.getMessageProperties().setDeliveryMode(MessageDeliveryMode.fromInt(Integer.parseInt(headerValueString)));
            } else if(TYPE.equals(headerKey)) {
                msg.getMessageProperties().setType(headerValueString);
            }
        }
        
        return msg;
    }
    
    public static SpringAMQPMessage setBasicPropertiesToHeaders(SpringAMQPMessage msg, Message amqpMessage) {
        msg.getHeaders().put(MESSAGE_ID, amqpMessage.getMessageProperties().getMessageId());
        byte[] correlationId = amqpMessage.getMessageProperties().getCorrelationId();
        msg.getHeaders().put(CORRELATION_ID, correlationId == null ? null : new String(correlationId));
        msg.getHeaders().put(CONTENT_ENCODING, amqpMessage.getMessageProperties().getContentEncoding());
        msg.getHeaders().put(CONTENT_TYPE, amqpMessage.getMessageProperties().getContentType());
        msg.getHeaders().put(EXPIRATION, amqpMessage.getMessageProperties().getExpiration());
        msg.getHeaders().put(PRIORITY, amqpMessage.getMessageProperties().getPriority());
        msg.getHeaders().put(REPLY_TO, amqpMessage.getMessageProperties().getReplyTo());
        msg.getHeaders().put(DELIVERY_MODE, MessageDeliveryMode.toInt(amqpMessage.getMessageProperties().getDeliveryMode()));
        msg.getHeaders().put(TYPE, amqpMessage.getMessageProperties().getType());

        return msg;
    }
    
    public static Message copyHeaders(Message msg, Map<String, Object> headers) {
        for(Map.Entry<String, Object> headerEntry : headers.entrySet()) {
            // headers used for setting basic properties and routing key are skipped
            if( !CONTENT_ENCODING.equals(headerEntry.getKey()) &&
                    !CONTENT_TYPE.equals(headerEntry.getKey()) &&
                    !CORRELATION_ID.equals(headerEntry.getKey()) &&
                    !EXPIRATION.equals(headerEntry.getKey()) &&
                    !PRIORITY.equals(headerEntry.getKey()) &&
                    !REPLY_TO.equals(headerEntry.getKey()) &&
                    !DELIVERY_MODE.equals(headerEntry.getKey()) &&
                    !TYPE.equals(headerEntry.getKey()) &&
                    !SpringAMQPComponent.ROUTING_KEY_HEADER.equals(headerEntry.getKey()) &&
                    !MESSAGE_ID.equals(headerEntry.getKey()) &&
                    !SpringAMQPComponent.EXCHANGE_NAME_HEADER.equals(headerEntry.getKey()) &&
                    !msg.getMessageProperties().getHeaders().containsKey(headerEntry.getKey())) {
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
