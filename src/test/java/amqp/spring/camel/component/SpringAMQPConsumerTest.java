/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */
package amqp.spring.camel.component;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.camel.CamelContext;
import org.apache.camel.Component;
import org.apache.camel.Consumer;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.JndiRegistry;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JsonMessageConverter;

public class SpringAMQPConsumerTest extends CamelTestSupport {

    @Test
    public void testCreateContext() throws Exception {
        Component component = context().getComponent(SpringAMQPComponent.DEFAULT_SCHEME, SpringAMQPComponent.class);
        Assert.assertNotNull(component);
    }

    @Test
    public void restartConsumer() throws Exception {
        Processor defaultProcessor = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
            }
        };

        Consumer amqpConsumer = context().getEndpoint(
                SpringAMQPComponent.DEFAULT_SCHEME
                        + ":directExchange:q0:test.a?durable=false&autodelete=true&exclusive=false&ha=true")
                .createConsumer(defaultProcessor);
        amqpConsumer.stop();
        amqpConsumer.start();
    }

    @Test
    public void disconnectConsumer() throws Exception {
        Processor defaultProcessor = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
            }
        };

        SpringAMQPConsumer amqpConsumer = (SpringAMQPConsumer) context().getEndpoint(
                SpringAMQPComponent.DEFAULT_SCHEME
                        + ":directExchange:q0:test.a?durable=false&autodelete=true&exclusive=false").createConsumer(
                defaultProcessor);
        amqpConsumer.onClose(null);
        amqpConsumer.onCreate(null);
    }

    @Test
    public void testKeyValueParsing() throws Exception {
        Map < String, Object > keyValues = SpringAMQPConsumer.parseKeyValues("cheese=gouda&fromage=jack");
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
        context().createProducerTemplate().sendBodyAndHeader(
                SpringAMQPComponent.DEFAULT_SCHEME
                        + ":directExchange:test.a?durable=false&autodelete=true&exclusive=false", "sendMessage",
                "HeaderKey", "HeaderValue");

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
        context().createProducerTemplate().asyncRequestBodyAndHeader(
                SpringAMQPComponent.DEFAULT_SCHEME
                        + ":directExchange:test.b?durable=false&autodelete=true&exclusive=false", "sendMessage",
                "HeaderKey", "HeaderValue");

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

        Map < String, Object > headersOne = new HashMap < String, Object >();
        headersOne.put("cheese", "asiago");
        headersOne.put("fromage", "cheddar");
        context().createProducerTemplate().sendBodyAndHeaders(
                SpringAMQPComponent.DEFAULT_SCHEME + ":headerAndExchange?type=headers", "testHeaderExchange",
                headersOne);

        MockEndpoint mockEndpointTwo = context().getEndpoint("mock:test.c", MockEndpoint.class);
        mockEndpointTwo.expectedMessageCount(1);

        Map < String, Object > headersTwo = new HashMap < String, Object >();
        headersTwo.put("cheese", "gouda");
        headersTwo.put("fromage", "jack");
        context().createProducerTemplate().sendBodyAndHeaders(
                SpringAMQPComponent.DEFAULT_SCHEME + ":headerAndExchange?type=headers", "testHeaderExchange",
                headersTwo);

        mockEndpointOne.assertIsSatisfied();
        mockEndpointTwo.assertIsSatisfied();
    }

    @Test
    public void testHeaderOrExchange() throws Exception {
        MockEndpoint mockEndpointOne = getMockEndpoint("mock:test.d");
        mockEndpointOne.expectedMessageCount(2);

        Map < String, Object > headersOne = new HashMap < String, Object >();
        headersOne.put("cheese", "asiago");
        headersOne.put("fromage", "bleu");
        context().createProducerTemplate()
                .sendBodyAndHeaders(SpringAMQPComponent.DEFAULT_SCHEME + ":headerOrExchange?type=headers",
                        "testHeaderExchange", headersOne);

        Map < String, Object > headersTwo = new HashMap < String, Object >();
        headersTwo.put("cheese", "white");
        headersTwo.put("fromage", "jack");
        context().createProducerTemplate()
                .sendBodyAndHeaders(SpringAMQPComponent.DEFAULT_SCHEME + ":headerOrExchange?type=headers",
                        "testHeaderExchange", headersTwo);

        mockEndpointOne.assertIsSatisfied();
    }

    @Test
    public void testDefaultExchange() throws Exception {
        MockEndpoint mockEndpointOne = getMockEndpoint("mock:test.e");
        mockEndpointOne.expectedMessageCount(1);

        context().createProducerTemplate().sendBody(SpringAMQPComponent.DEFAULT_SCHEME + "::test.e", "testBody");

        mockEndpointOne.assertIsSatisfied();
    }

    @Test
    public void sendMessageTTL() throws Exception {
        MockEndpoint mockEndpoint = getMockEndpoint("mock:test.a");
        mockEndpoint.expectedMessageCount(1);
        context().createProducerTemplate().sendBodyAndHeader(
                SpringAMQPComponent.DEFAULT_SCHEME
                        + ":directExchange:test.a?durable=false&autodelete=true&exclusive=false&timeToLive=1000",
                "sendMessage", "HeaderKey", "HeaderValue");

        mockEndpoint.assertIsSatisfied();
        Message inMessage = mockEndpoint.getExchanges().get(0).getIn();
        Assert.assertEquals("sendMessage", inMessage.getBody(String.class));
        Assert.assertEquals("HeaderValue", inMessage.getHeader("HeaderKey"));
    }

    @Test
    public void testHandleException() {
        try {
            Object result = context().createProducerTemplate().requestBody(
                    SpringAMQPComponent.DEFAULT_SCHEME + "::test.f", "testBody");
            Assert.fail("Should have thrown exception up to caller but received object: " + result);
        } catch (RuntimeException e) {
            // success
        }
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
        // The JSON converter stresses marshalling more than the default converter
        amqpTemplate.setMessageConverter(new JsonMessageConverter());
        SpringAMQPComponent amqpComponent = new SpringAMQPComponent(factory);
        amqpComponent.setAmqpTemplate(amqpTemplate);

        CamelContext camelContext = super.createCamelContext();
        camelContext.addComponent(SpringAMQPComponent.DEFAULT_SCHEME, amqpComponent);
        return camelContext;
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from(
                        SpringAMQPComponent.DEFAULT_SCHEME
                                + ":directExchange:q1:test.a?durable=false&autodelete=true&exclusive=false").to(
                        "mock:test.a");
                from(
                        SpringAMQPComponent.DEFAULT_SCHEME
                                + ":directExchange:q5:test.b?durable=false&autodelete=true&exclusive=false").to(
                        "mock:test.b");
                from(
                        SpringAMQPComponent.DEFAULT_SCHEME
                                + ":headerAndExchange:q2:cheese=asiago&fromage=cheddar?type=headers&durable=false&autodelete=true&exclusive=false")
                        .to("mock:test.b");
                from(
                        SpringAMQPComponent.DEFAULT_SCHEME
                                + ":headerAndExchange:q3:cheese=gouda&fromage=jack?type=headers&durable=false&autodelete=true&exclusive=false")
                        .to("mock:test.c");
                from(
                        SpringAMQPComponent.DEFAULT_SCHEME
                                + ":headerOrExchange:q4:cheese=white|fromage=bleu?type=headers&durable=false&autodelete=true&exclusive=false")
                        .to("mock:test.d");
                from(
                        SpringAMQPComponent.DEFAULT_SCHEME
                                + "::test.e:test.e?durable=false&autodelete=true&exclusive=false").to("mock:test.e");
                from(
                        SpringAMQPComponent.DEFAULT_SCHEME
                                + "::test.f:test.f?durable=false&autodelete=true&exclusive=false").beanRef(
                        "exceptionThrower", "explode");
            }
        };
    }
}
