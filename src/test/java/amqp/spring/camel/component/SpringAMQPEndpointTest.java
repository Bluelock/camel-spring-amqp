/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */
package amqp.spring.camel.component;

import org.apache.camel.*;
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
        Component component = context().getComponent("spring-amqp", SpringAMQPComponent.class);
    	String remaining = ":queue1:routingKey1";
        String uri = "spring-amqp:"+remaining;
    	
    	SpringAMQPEndpoint endpoint = new SpringAMQPEndpoint(component, uri, remaining, null, null);
    	
    	Assert.assertEquals("", endpoint.getExchangeName());
    	Assert.assertEquals("queue1", endpoint.getQueueName());
    	Assert.assertEquals("routingKey1", endpoint.getRoutingKey());
    }
    
    @Test
    public void testUriParsingOfDefaultExchangeWithRoutingKeyForProducer() {
        Component component = context().getComponent("spring-amqp", SpringAMQPComponent.class);
        String remaining = ":routingKey1";
        String uri = "spring-amqp:"+remaining;
    	
    	SpringAMQPEndpoint endpoint = new SpringAMQPEndpoint(component, uri, remaining, null, null);
    	
    	Assert.assertEquals("", endpoint.getExchangeName());
    	Assert.assertEquals("routingKey1", ReflectionTestUtils.getField(endpoint, "tempQueueOrKey"));
    }
    
    @Test
    public void testIsUsingDefaultExchangeTrue() {
        Component component = context().getComponent("spring-amqp", SpringAMQPComponent.class);
        String remaining = ":routingKey1";
        String uri = "spring-amqp:"+remaining;
    	
    	SpringAMQPEndpoint endpoint = new SpringAMQPEndpoint(component, uri, remaining, null, null);
    	
    	Assert.assertTrue(endpoint.isUsingDefaultExchange());
    }
    
    @Test
    public void testIsNotUsingDefaultExchangeFalse() {
        Component component = context().getComponent("spring-amqp", SpringAMQPComponent.class);
        String remaining = "exchange1:routingKey1";
        String uri = "spring-amqp:"+remaining;
    	
    	SpringAMQPEndpoint endpoint = new SpringAMQPEndpoint(component, uri, remaining, null, null);
    	
    	Assert.assertFalse(endpoint.isUsingDefaultExchange());
    }    
        
    @Test
    public void testDefaultFanoutConsumer() throws Exception {
        Processor defaultProcessor = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception { }
        };

        Component component = context().getComponent("spring-amqp", SpringAMQPComponent.class);
        String remaining = "exchange2:queue2";
        String uri = "spring-amqp:"+remaining;
    	
    	SpringAMQPEndpoint endpoint = new SpringAMQPEndpoint(component, uri, remaining, null, null);
        endpoint.createConsumer(defaultProcessor);
    	
        //If you specify an exchange and queue but nothing else, this should be a fanout exchange
    	Assert.assertEquals("queue2", endpoint.getQueueName());
        Assert.assertEquals("exchange2", endpoint.getExchangeName());
        Assert.assertEquals("fanout", endpoint.getType());
    }
    
    @Test
    public void testHashDelimiters() {
        Component component = context().getComponent("spring-amqp", SpringAMQPComponent.class);
        String remaining = "exchange1:#.routingKey1.#";
        String uri = "spring-amqp:"+remaining;
    	
    	SpringAMQPEndpoint endpoint = new SpringAMQPEndpoint(component, uri, remaining, null, null);
    	
        //Ensure things can be printed correctly; setEndpoint(String) has had issues previously
    	Assert.assertNotNull(endpoint.toString());
    }    
        
    @Override
    protected CamelContext createCamelContext() throws Exception {
        ConnectionFactory factory = new CachingConnectionFactory();
        
        CamelContext camelContext = super.createCamelContext();
        camelContext.addComponent("spring-amqp", new SpringAMQPComponent(factory));
        return camelContext;
    }
}
