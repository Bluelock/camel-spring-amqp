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
import org.apache.camel.ExchangePattern;
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
    
    public static final String EXCHANGE_PATTERN = "CamelExchangePattern";
    
    public SpringAMQPMessage() {
        super();
    }

    public SpringAMQPMessage(org.apache.camel.Message source) {
        super();
        if(source != null) copyFrom(source);
    }

    public static SpringAMQPMessage fromAMQPMessage(MessageConverter msgConverter, org.springframework.amqp.core.Message amqpMessage) {
        if(amqpMessage == null) {
            LOG.warn("Received NULL AMQP Message, returning null");
            return null;
        }
        
        SpringAMQPMessage message = new SpringAMQPMessage();
        
        //Restore the body based on the message converter provided
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
        
        //We have the body, now restore the headers
        for(Entry<String, Object> headerEntry : amqpMessage.getMessageProperties().getHeaders().entrySet()) {
            if(! EXCHANGE_PATTERN.equals(headerEntry.getKey())) {
                message.setHeader(headerEntry.getKey(), headerEntry.getValue());
            }
        }
        
        return message;
    }
    
    public static ExchangePattern getExchangePattern(org.springframework.amqp.core.Message amqpMessage) {
        String exchangePatternName;
        
        exchangePatternName = (String) amqpMessage.getMessageProperties().getHeaders().get(EXCHANGE_PATTERN);
        if(exchangePatternName == null) //Safe default
            exchangePatternName = ExchangePattern.InOptionalOut.name();
        
        return ExchangePattern.valueOf(exchangePatternName);
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
                        
            for(Entry<String, Object> headerEntry : camelMessage.getHeaders().entrySet()) {
                if(! msg.getMessageProperties().getHeaders().containsKey(headerEntry.getKey()))
                    msg.getMessageProperties().setHeader(headerEntry.getKey(), headerEntry.getValue());
            }
            
            //Set the exchange pattern so we can re-set it upon receipt
            if(camelMessage.getExchange() != null) {
                String exchangePattern = camelMessage.getExchange().getPattern().name();
                msg.getMessageProperties().setHeader(EXCHANGE_PATTERN, exchangePattern);
            } else {
                throw new IllegalStateException("No exchange was found for this message "+camelMessage.getMessageId());
            }
            
            return msg;
        }
    }
}
