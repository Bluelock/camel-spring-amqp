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
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JsonMessageConverter;

//TODO Try having unit tests talk to a VM local AMQP broker (like a Qpid broker)
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
        
        Consumer amqpConsumer = context().getEndpoint("spring-amqp:directExchange:q0:test.a?durable=false&autodelete=true&exclusive=false").createConsumer(defaultProcessor);
        amqpConsumer.start();
        amqpConsumer.stop();
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
        MockEndpoint mockEndpoint = context().getEndpoint("mock:test.a", MockEndpoint.class);
        mockEndpoint.expectedMessageCount(1);
        context().createProducerTemplate().sendBodyAndHeader("spring-amqp:directExchange:test.a?durable=false&autodelete=true&exclusive=false", "sendMessage", "HeaderKey", "HeaderValue");
        
        mockEndpoint.assertIsSatisfied();
        Message inMessage = mockEndpoint.getExchanges().get(0).getIn();
        Assert.assertEquals("sendMessage", inMessage.getBody(String.class));
        Assert.assertEquals("HeaderValue", inMessage.getHeader("HeaderKey"));
    }
    
    @Test
    public void testHeaderAndExchange() throws Exception {
        MockEndpoint mockEndpointOne = context().getEndpoint("mock:test.b", MockEndpoint.class);
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
        MockEndpoint mockEndpointOne = context().getEndpoint("mock:test.d", MockEndpoint.class);
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
    public void sendMessageTTL() throws Exception {
        MockEndpoint mockEndpoint = context().getEndpoint("mock:test.a", MockEndpoint.class);
        mockEndpoint.expectedMessageCount(1);
        context().createProducerTemplate().sendBodyAndHeader("spring-amqp:directExchange:test.a?durable=false&autodelete=true&exclusive=false&timeToLive=1000", "sendMessage", "HeaderKey", "HeaderValue");
        
        mockEndpoint.assertIsSatisfied();
        Message inMessage = mockEndpoint.getExchanges().get(0).getIn();
        Assert.assertEquals("sendMessage", inMessage.getBody(String.class));
        Assert.assertEquals("HeaderValue", inMessage.getHeader("HeaderKey"));
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
                from("spring-amqp:headerAndExchange:q2:cheese=asiago&fromage=cheddar?type=headers&durable=false&autodelete=true&exclusive=false").to("mock:test.b");
                from("spring-amqp:headerAndExchange:q3:cheese=gouda&fromage=jack?type=headers&durable=false&autodelete=true&exclusive=false").to("mock:test.c");
                from("spring-amqp:headerOrExchange:q4:cheese=white|fromage=bleu?type=headers&durable=false&autodelete=true&exclusive=false").to("mock:test.d");
            }
        };
    }
}
