/**
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is camel-spring-amqp.
 * 
 * The Initial Developer of the Original Code is Bluelock, LLC.
 * Copyright (c) 2007-2011 Bluelock, LLC. All Rights Reserved.
 */
package amqp.spring.camel.component;

import java.util.Map.Entry;
import org.apache.camel.impl.DefaultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConverter;

public class SpringAMQPMessage extends DefaultMessage {
    private static transient final Logger LOG = LoggerFactory.getLogger(SpringAMQPMessage.class);
    
    public SpringAMQPMessage() {
        super();
    }

    public static SpringAMQPMessage fromAMQPMessage(MessageConverter msgConverter, org.springframework.amqp.core.Message amqpMessage) {
        if(amqpMessage == null) {
            LOG.warn("Received NULL AMQP Message, returning null");
            return null;
        }
        
        SpringAMQPMessage message = new SpringAMQPMessage();
        
        if(amqpMessage.getBody() == null || 
                (amqpMessage.getBody() instanceof byte[] && ((byte[]) amqpMessage.getBody()).length == 0)) {
            message.setBody(null);
        } else {
            if(LOG.isTraceEnabled()) {
                String asText = new String(amqpMessage.getBody());
                LOG.trace("Translating From AMQP Message: "+asText);
            }
            
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
            
            if(LOG.isTraceEnabled()) {
                String asText = new String(amqpMessage.getBody());
                LOG.trace("Translating To AMQP Message: "+asText);
            }
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
