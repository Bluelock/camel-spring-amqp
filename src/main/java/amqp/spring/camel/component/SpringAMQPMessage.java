package amqp.spring.camel.component;

import java.util.Map.Entry;
import org.apache.camel.impl.DefaultMessage;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConverter;

public class SpringAMQPMessage extends DefaultMessage {
    public SpringAMQPMessage() {
        super();
    }

    public static SpringAMQPMessage fromAMQPMessage(MessageConverter msgConverter, org.springframework.amqp.core.Message amqpMessage) {
        SpringAMQPMessage message = new SpringAMQPMessage();
        
        if(amqpMessage.getBody() == null || 
                (amqpMessage.getBody() instanceof byte[] && ((byte[]) amqpMessage.getBody()).length == 0)) {
            message.setBody(null);
        } else {
            message.setBody(msgConverter.fromMessage(amqpMessage));
        }
        
        for(Entry<String, Object> headerEntry : amqpMessage.getMessageProperties().getHeaders().entrySet())
            message.setHeader(headerEntry.getKey(), headerEntry.getValue());
        
        return message;
    }

    public Message toAMQPMessage(MessageConverter msgConverter) {
        MessageProperties properties = new MessageProperties();
        properties.setMessageId(this.getMessageId());
        
        Message amqpMessage;
        if(this.getBody() != null) {
            amqpMessage = msgConverter.toMessage(this.getBody(), properties);
        } else {
            amqpMessage = new Message(new byte[]{}, properties);
        }
        
        return new HeadersPostProcessor(this).postProcessMessage(amqpMessage);
    }
    
    public static class HeadersPostProcessor implements MessagePostProcessor {
        public org.apache.camel.Message camelMessage;
        
        public HeadersPostProcessor(org.apache.camel.Message camelMessage) {
            this.camelMessage = camelMessage;
        }
        
        @Override
        public Message postProcessMessage(Message msg) throws AmqpException {
            if(camelMessage == null || camelMessage.getHeaders() == null)
                return msg;
                        
            for(Entry<String, Object> headerEntry : camelMessage.getHeaders().entrySet())
                msg.getMessageProperties().setHeader(headerEntry.getKey(), headerEntry.getValue());
            
            return msg;
        }
    }
}
