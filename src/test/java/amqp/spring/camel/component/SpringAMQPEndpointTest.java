/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */
package amqp.spring.camel.component;

import org.apache.camel.CamelContext;
import org.apache.camel.Component;
import org.apache.camel.Endpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.test.util.ReflectionTestUtils;

public class SpringAMQPEndpointTest extends CamelTestSupport {
    
    @Test
    public void testCreateContext() throws Exception {
        Component component = context().getComponent("spring-amqp", SpringAMQPComponent.class);
        Assert.assertNotNull(component);
        
        Endpoint endpoint = component.createEndpoint("spring-amqp:test.a");
        Assert.assertNotNull(endpoint);
    }
    
    @Test
    public void testUriParsingOfDefaultExchangeWithQueueAndRoutingKeyForConsumer() {
    	String remaining = ":queue1:routingKey1";
    	
    	SpringAMQPEndpoint endpoint = new SpringAMQPEndpoint(remaining, null, null, null);
    	
    	Assert.assertEquals("", endpoint.exchangeName);
    	Assert.assertEquals("queue1", endpoint.queueName);
    	Assert.assertEquals("routingKey1", endpoint.routingKey);
    }
    
    @Test
    public void testUriParsingOfDefaultExchangeWithRoutingKeyForProducer() {
        String remaining = ":routingKey1";
    	
    	SpringAMQPEndpoint endpoint = new SpringAMQPEndpoint(remaining, null, null, null);
    	
    	Assert.assertEquals("", endpoint.exchangeName);
    	Assert.assertEquals("routingKey1", ReflectionTestUtils.getField(endpoint, "tempQueueOrKey"));
    }
    
    @Test
    public void testIsUsingDefaultExchangeTrue() {
        String remaining = ":routingKey1";
    	
    	SpringAMQPEndpoint endpoint = new SpringAMQPEndpoint(remaining, null, null, null);
    	
    	Assert.assertTrue(endpoint.isUsingDefaultExchange());
    }
    
    @Test
    public void testIsNotUsingDefaultExchangeFalse() {
        String remaining = "exchange1:routingKey1";
    	
    	SpringAMQPEndpoint endpoint = new SpringAMQPEndpoint(remaining, null, null, null);
    	
    	Assert.assertFalse(endpoint.isUsingDefaultExchange());
    }    
        
    @Override
    protected CamelContext createCamelContext() throws Exception {
        ConnectionFactory factory = new CachingConnectionFactory();
        
        CamelContext camelContext = super.createCamelContext();
        camelContext.addComponent("spring-amqp", new SpringAMQPComponent(factory));
        return camelContext;
    }
}
