/**
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License version 2.0 (the "License"). You can obtain a copy of the
 * License at http://mozilla.org/MPL/2.0/.
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

import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.apache.camel.CamelContext;
import org.apache.camel.Component;
import org.apache.camel.Consumer;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;

//TODO Try having unit tests talk to a VM local AMQP broker (like a Qpid broker)
public class SpringAMQPConsumerTest extends CamelTestSupport {
    private CachingConnectionFactory factory;
    
    @Test
    public void testCreateContext() throws Exception {
        Component component = context().getComponent("spring-amqp", SpringAMQPComponent.class);
        Assert.assertNotNull(component);
    }
    
//    @Test 
    public void restartConsumer() throws Exception {
        Processor defaultProcessor = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception { }
        };
        
        Consumer amqpConsumer = context().getEndpoint("spring-amqp:directExchange:q0:test.a?durable=false&autodelete=true&exclusive=false").createConsumer(defaultProcessor);
        amqpConsumer.start();
        amqpConsumer.stop();
    }
    
    @Test
    public void testKeyValueParsing() throws Exception {
        Map<String, Object> keyValues = SpringAMQPConsumer.parseKeyValues("cheese=gouda&fromage=jack");
        Assert.assertEquals("gouda", keyValues.get("cheese"));
        Assert.assertEquals("jack", keyValues.get("fromage"));
    }

    @Test
    public void sendMessage() throws Exception {
        MockEndpoint mockEndpoint = context().getEndpoint("mock:test.a", MockEndpoint.class);
        mockEndpoint.expectedMessageCount(1);
        context().createProducerTemplate().sendBody("spring-amqp:directExchange:test.a?durable=false&autodelete=true&exclusive=false", "sendMessage");
        mockEndpoint.assertIsSatisfied();
    }
    
//    @Test
    public void testHeaderExchange() throws Exception {
        MockEndpoint mockEndpointOne = context().getEndpoint("mock:test.b", MockEndpoint.class);
        mockEndpointOne.expectedMessageCount(1);
        
        Map<String, Object> headersOne = new HashMap<String, Object>();
        headersOne.put("cheese", "asiago");
        headersOne.put("fromage", "cheddar");
        context().createProducerTemplate().sendBodyAndHeaders("spring-amqp:headerExchange?type=headers", "testHeaderExchange", headersOne);
        
        MockEndpoint mockEndpointTwo = context().getEndpoint("mock:test.b", MockEndpoint.class);
        mockEndpointTwo.expectedMessageCount(1);
        
        Map<String, Object> headersTwo = new HashMap<String, Object>();
        headersTwo.put("cheese", "gouda");
        headersTwo.put("fromage", "jack");
        context().createProducerTemplate().sendBodyAndHeaders("spring-amqp:headerExchange?type=headers", "testHeaderExchange", headersTwo);
        
        mockEndpointOne.assertIsSatisfied();
        mockEndpointTwo.assertIsSatisfied();
    }
    
    @Override
    protected CamelContext createCamelContext() throws Exception {
        this.factory = new CachingConnectionFactory();
        CamelContext camelContext = super.createCamelContext();
        camelContext.addComponent("spring-amqp", new SpringAMQPComponent(this.factory));
        return camelContext;
    }

    @Override
    protected void stopCamelContext() throws Exception {
        super.stopCamelContext();
        this.factory.destroy();
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("spring-amqp:directExchange:q1:test.a?durable=false&autodelete=true&exclusive=false").to("mock:test.a");
//                from("spring-amqp:headerExchange:q2:cheese=asiago&fromage=cheddar?type=headers&durable=false&autodelete=true&exclusive=false").to("mock:test.b");
//                from("spring-amqp:headerExchange:q3:cheese=gouda&fromage=jack?type=headers&durable=false&autodelete=true&exclusive=false").to("mock:test.c");
            }
        };
    }
}
