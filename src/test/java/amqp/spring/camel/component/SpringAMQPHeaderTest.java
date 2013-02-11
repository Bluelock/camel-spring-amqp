/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */
package amqp.spring.camel.component;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.DefaultExchange;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

public class SpringAMQPHeaderTest {
    @Test
    public void fromBasicProperties() throws Exception {
        MessageProperties properties = new MessageProperties();
        properties.setHeader("NotSecret", "Popcorn");
        org.springframework.amqp.core.Message message = new Message(new byte[]{}, properties);
        message.getMessageProperties().setPriority(1);
        message.getMessageProperties().setReplyTo("BuzzSaw");
        
        SpringAMQPMessage camelMessage = SpringAMQPHeader.setBasicPropertiesToHeaders(new SpringAMQPMessage(), message);
        Assert.assertNull(camelMessage.getHeader("NotSecret"));
        Assert.assertEquals(1, camelMessage.getHeader(SpringAMQPHeader.PRIORITY));
        Assert.assertEquals("BuzzSaw", camelMessage.getHeader(SpringAMQPHeader.REPLY_TO));
    }
    
    @Test
    public void toBasicProperties() throws Exception {
        SpringAMQPMessage camelMessage = new SpringAMQPMessage();
        camelMessage.setHeader("Secret", "My Secret");
        camelMessage.setHeader(SpringAMQPHeader.PRIORITY, 1);
        camelMessage.setHeader(SpringAMQPHeader.REPLY_TO, "BuzzSaw");
        
        Exchange exchange = new DefaultExchange(new DefaultCamelContext());
        exchange.setIn(camelMessage);
        
        Message message = new Message(new byte[]{}, new MessageProperties());
        message = SpringAMQPHeader.setBasicPropertiesFromHeaders(message, camelMessage.getHeaders());
        Assert.assertNull(message.getMessageProperties().getHeaders().get("Secret"));
        Assert.assertEquals(Integer.valueOf(1), message.getMessageProperties().getPriority());
        Assert.assertEquals("BuzzSaw", message.getMessageProperties().getReplyTo());
    }
    
    @Test
    public void copyAMQPHeaders() throws Exception {
        MessageProperties properties = new MessageProperties();
        properties.setHeader("NotSecret", "Popcorn");
        org.springframework.amqp.core.Message message = new Message(new byte[]{}, properties);
        message.getMessageProperties().setReplyTo("BuzzSaw");
        
        SpringAMQPMessage camelMessage = SpringAMQPHeader.copyHeaders(new SpringAMQPMessage(), message.getMessageProperties().getHeaders());
        Assert.assertEquals("Popcorn", camelMessage.getHeader("NotSecret"));
        Assert.assertNull(camelMessage.getHeader(SpringAMQPHeader.REPLY_TO));
    }
    
    @Test
    public void copyCamelHeaders() throws Exception {
        SpringAMQPMessage camelMessage = new SpringAMQPMessage();
        camelMessage.setHeader("Secret", "My Secret");
        camelMessage.setHeader(SpringAMQPHeader.REPLY_TO, "BuzzSaw");
        
        Exchange exchange = new DefaultExchange(new DefaultCamelContext());
        exchange.setIn(camelMessage);
        
        Message message = new Message(new byte[]{}, new MessageProperties());
        message = SpringAMQPHeader.copyHeaders(message, camelMessage.getHeaders());
        Assert.assertEquals("My Secret", message.getMessageProperties().getHeaders().get("Secret"));
        Assert.assertNull(message.getMessageProperties().getReplyTo());
    }
}
