/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */
package amqp.spring.camel.component;

import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.DefaultExchange;
import org.apache.camel.impl.DefaultMessage;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.AbstractMessageConverter;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;

public class SpringAMQPMessageTest {
    @Test
    public void testExchangePattern() throws Exception {
        org.apache.camel.Message camelMessage = new DefaultMessage();
        Exchange exchange = new DefaultExchange(new DefaultCamelContext(), ExchangePattern.InOut);
        exchange.setIn(camelMessage);
        
        MessageProperties properties = new MessageProperties();
        org.springframework.amqp.core.Message amqpMessage = new org.springframework.amqp.core.Message("Testing".getBytes(), properties);
        
        amqpMessage = new SpringAMQPMessage.HeadersPostProcessor(camelMessage).postProcessMessage(amqpMessage);
        ExchangePattern exchangePattern = SpringAMQPMessage.getExchangePattern(amqpMessage);
        Assert.assertEquals(exchange.getPattern(), exchangePattern);
    }
    
    @Test
    public void fromAMQP() throws Exception {
        String body = "Test Message";
        MessageConverter msgConverter = new StringMessageConverter();
        MessageProperties properties = new MessageProperties();
        properties.setHeader("NotSecret", "Popcorn");
        org.springframework.amqp.core.Message message = new org.springframework.amqp.core.Message(body.getBytes(), properties);
        
        SpringAMQPMessage camelMessage = SpringAMQPMessage.fromAMQPMessage(msgConverter, message);
        Assert.assertEquals(body, camelMessage.getBody(String.class));
        Assert.assertEquals("Popcorn", camelMessage.getHeader("NotSecret"));
    }
    
    @Test
    public void toAMQP() throws Exception {
        MessageConverter msgConverter = new StringMessageConverter();
        
        SpringAMQPMessage camelMessage = new SpringAMQPMessage();
        camelMessage.setBody("Test Message 2");
        camelMessage.setHeader("Secret", "My Secret");
        
        Exchange exchange = new DefaultExchange(new DefaultCamelContext());
        exchange.setIn(camelMessage);
        
        org.springframework.amqp.core.Message message = camelMessage.toAMQPMessage(msgConverter);
        Assert.assertEquals("Test Message 2", new String(message.getBody()));
        Assert.assertEquals("My Secret", message.getMessageProperties().getHeaders().get("Secret"));
    }
    
    private static class StringMessageConverter extends AbstractMessageConverter {
        @Override
        protected org.springframework.amqp.core.Message createMessage(Object object, MessageProperties messageProperties) {
            return new org.springframework.amqp.core.Message(((String) object).getBytes(), messageProperties);
        }

        @Override
        public Object fromMessage(org.springframework.amqp.core.Message message) throws MessageConversionException {
            return new String(message.getBody());
        }
    }
}
