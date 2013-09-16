/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */
package amqp.spring.camel.component;

import junit.framework.Assert;
import org.apache.camel.*;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.JndiRegistry;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JsonMessageConverter;

import java.util.HashMap;
import java.util.Map;


public class SpringAMQPConsumerTest extends CamelTestSupport {
    
    @Test
    public void testCreateContext() throws Exception {
        Component component = context().getComponent("spring-amqp", SpringAMQPComponent.class);
        Assert.assertNotNull(component);
    }
    
    @Test 
    public void restartConsumer() throws Exception {
        Processor defaultProcessor = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception { }
        };
        
        Consumer amqpConsumer = context().getEndpoint("spring-amqp:directExchange:q0:test.a?durable=false&autodelete=true&exclusive=false&ha=true").createConsumer(defaultProcessor);
        amqpConsumer.stop();
        amqpConsumer.start();
    }
    
    @Test 
    public void disconnectConsumer() throws Exception {
        Processor defaultProcessor = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception { }
        };
        
        SpringAMQPConsumer amqpConsumer = (SpringAMQPConsumer) context().getEndpoint("spring-amqp:directExchange:q0:test.a?durable=false&autodelete=true&exclusive=false").createConsumer(defaultProcessor);
        amqpConsumer.onClose(null);
        amqpConsumer.onCreate(null);
    }
    
    @Test
    public void testKeyValueParsing() throws Exception {
        Map<String, Object> keyValues = SpringAMQPConsumer.parseKeyValues("cheese=gouda&fromage=jack");
        Assert.assertEquals("gouda", keyValues.get("cheese"));
        Assert.assertEquals("jack", keyValues.get("fromage"));

        keyValues = SpringAMQPConsumer.parseKeyValues("cheese=gouda|fromage=jack");
        Assert.assertEquals("gouda", keyValues.get("cheese"));
        Assert.assertEquals("jack", keyValues.get("fromage"));
    }

    @Test
    public void sendMessage() throws Exception {
        MockEndpoint mockEndpoint = getMockEndpoint("mock:test.a");
        mockEndpoint.expectedMessageCount(1);
        context().createProducerTemplate().sendBodyAndHeader("spring-amqp:directExchange:test.a?durable=false&autodelete=true&exclusive=false", "sendMessage", "HeaderKey", "HeaderValue");
        
        mockEndpoint.assertIsSatisfied();
        Message inMessage = mockEndpoint.getExchanges().get(0).getIn();
        Assert.assertEquals("sendMessage", inMessage.getBody(String.class));
        Assert.assertEquals("HeaderValue", inMessage.getHeader("HeaderKey"));
        Assert.assertNotNull(inMessage.getMessageId());
    }
    
    @Test
    public void sendAsyncMessage() throws Exception {
        MockEndpoint mockEndpoint = getMockEndpoint("mock:test.b");
        mockEndpoint.expectedMessageCount(1);
        context().createProducerTemplate().asyncRequestBodyAndHeader("spring-amqp:directExchange:test.b?durable=false&autodelete=true&exclusive=false", "sendMessage", "HeaderKey", "HeaderValue");
        
        mockEndpoint.assertIsSatisfied();
        Message inMessage = mockEndpoint.getExchanges().get(0).getIn();
        Assert.assertEquals("sendMessage", inMessage.getBody(String.class));
        Assert.assertEquals("HeaderValue", inMessage.getHeader("HeaderKey"));
        Assert.assertNotNull(inMessage.getMessageId());
    }
    
    @Test
    public void testHeaderAndExchange() throws Exception {
        MockEndpoint mockEndpointOne = getMockEndpoint("mock:test.b");
        mockEndpointOne.expectedMessageCount(1);
        
        Map<String, Object> headersOne = new HashMap<String, Object>();
        headersOne.put("cheese", "asiago");
        headersOne.put("fromage", "cheddar");
        context().createProducerTemplate().sendBodyAndHeaders("spring-amqp:headerAndExchange?type=headers", "testHeaderExchange", headersOne);
        
        MockEndpoint mockEndpointTwo = context().getEndpoint("mock:test.c", MockEndpoint.class);
        mockEndpointTwo.expectedMessageCount(1);
        
        Map<String, Object> headersTwo = new HashMap<String, Object>();
        headersTwo.put("cheese", "gouda");
        headersTwo.put("fromage", "jack");
        context().createProducerTemplate().sendBodyAndHeaders("spring-amqp:headerAndExchange?type=headers", "testHeaderExchange", headersTwo);
        
        mockEndpointOne.assertIsSatisfied();
        mockEndpointTwo.assertIsSatisfied();
    }
    
    @Test
    public void testHeaderOrExchange() throws Exception {
        MockEndpoint mockEndpointOne = getMockEndpoint("mock:test.d");
        mockEndpointOne.expectedMessageCount(2);
        
        Map<String, Object> headersOne = new HashMap<String, Object>();
        headersOne.put("cheese", "asiago");
        headersOne.put("fromage", "bleu");
        context().createProducerTemplate().sendBodyAndHeaders("spring-amqp:headerOrExchange?type=headers", "testHeaderExchange", headersOne);
        
        Map<String, Object> headersTwo = new HashMap<String, Object>();
        headersTwo.put("cheese", "white");
        headersTwo.put("fromage", "jack");
        context().createProducerTemplate().sendBodyAndHeaders("spring-amqp:headerOrExchange?type=headers", "testHeaderExchange", headersTwo);
        
        mockEndpointOne.assertIsSatisfied();
    }
    
    @Test
    public void testDefaultExchange() throws Exception {
        MockEndpoint mockEndpointOne = getMockEndpoint("mock:test.e");
        mockEndpointOne.expectedMessageCount(1);
        
        context().createProducerTemplate().sendBody("spring-amqp::test.e", "testBody");
        
        mockEndpointOne.assertIsSatisfied();
    }
    
    @Test
    public void sendMessageTTL() throws Exception {
        MockEndpoint mockEndpoint = getMockEndpoint("mock:test.a");
        mockEndpoint.expectedMessageCount(1);
        context().createProducerTemplate().sendBodyAndHeader("spring-amqp:directExchange:test.a?durable=false&autodelete=true&exclusive=false&timeToLive=1000", "sendMessage", "HeaderKey", "HeaderValue");
        
        mockEndpoint.assertIsSatisfied();
        Message inMessage = mockEndpoint.getExchanges().get(0).getIn();
        Assert.assertEquals("sendMessage", inMessage.getBody(String.class));
        Assert.assertEquals("HeaderValue", inMessage.getHeader("HeaderKey"));
    }

    @Test
    public void testHandleException() {
        try {
            Object result = context().createProducerTemplate().requestBody("spring-amqp::test.f", "testBody");
            Assert.fail("Should have thrown exception up to caller but received object: " + result);
        } catch (RuntimeException e) {
            // success
        }
    }

    @Test
    public void testWithPrefetchParameter() throws Exception {
        MockEndpoint mockEndpoint = getMockEndpoint("mock:test.g");
        mockEndpoint.expectedMessageCount(2);
        ProducerTemplate producerTemplate = context().createProducerTemplate();

        producerTemplate.sendBodyAndHeader("spring-amqp:directExchange:test.g?durable=false&autodelete=true&exclusive=false&prefetchCount=1", "sendMessage1", "HeaderKey", "HeaderValue");
        producerTemplate.sendBodyAndHeader("spring-amqp:directExchange:test.g?durable=false&autodelete=true&exclusive=false&prefetchCount=1", "sendMessage2", "HeaderKey", "HeaderValue");

        mockEndpoint.assertIsSatisfied();

        Message inMessage1 = mockEndpoint.getExchanges().get(0).getIn();
        Assert.assertEquals("sendMessage1", inMessage1.getBody(String.class));
        Assert.assertEquals("HeaderValue", inMessage1.getHeader("HeaderKey"));

        Message inMessage2 = mockEndpoint.getExchanges().get(1).getIn();
        Assert.assertEquals("sendMessage2", inMessage2.getBody(String.class));
        Assert.assertEquals("HeaderValue", inMessage2.getHeader("HeaderKey"));

    }

    @Override
    protected JndiRegistry createRegistry() throws Exception {
        JndiRegistry registry = super.createRegistry();
        registry.bind("exceptionThrower", new TestExceptionThrower());
        return registry;
    }

    @Override
    protected CamelContext createCamelContext() throws Exception {
        CachingConnectionFactory factory = new CachingConnectionFactory();
        RabbitTemplate amqpTemplate = new RabbitTemplate(factory);
        //The JSON converter stresses marshalling more than the default converter
        amqpTemplate.setMessageConverter(new JsonMessageConverter());
        SpringAMQPComponent amqpComponent = new SpringAMQPComponent(factory);
        amqpComponent.setAmqpTemplate(amqpTemplate);
        
        CamelContext camelContext = super.createCamelContext();
        camelContext.addComponent("spring-amqp", amqpComponent);
        return camelContext;
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("spring-amqp:directExchange:q1:test.a?durable=false&autodelete=true&exclusive=false").to("mock:test.a");
                from("spring-amqp:directExchange:q5:test.b?durable=false&autodelete=true&exclusive=false").to("mock:test.b");
                from("spring-amqp:directExchange:q6:test.g?durable=false&prefetchCount=1&acknowledgeMode=auto").to("mock:test.g");
                from("spring-amqp:headerAndExchange:q2:cheese=asiago&fromage=cheddar?type=headers&durable=false&autodelete=true&exclusive=false").to("mock:test.b");
                from("spring-amqp:headerAndExchange:q3:cheese=gouda&fromage=jack?type=headers&durable=false&autodelete=true&exclusive=false").to("mock:test.c");
                from("spring-amqp:headerOrExchange:q4:cheese=white|fromage=bleu?type=headers&durable=false&autodelete=true&exclusive=false").to("mock:test.d");
                from("spring-amqp::test.e:test.e?durable=false&autodelete=true&exclusive=false").to("mock:test.e");
                from("spring-amqp::test.f:test.f?durable=false&autodelete=true&exclusive=false").beanRef("exceptionThrower", "explode");
            }
        };
    }
}

