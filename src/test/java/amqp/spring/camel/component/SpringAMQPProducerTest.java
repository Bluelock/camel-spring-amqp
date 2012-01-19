/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */
package amqp.spring.camel.component;

import java.io.Serializable;
import junit.framework.Assert;
import org.apache.camel.CamelContext;
import org.apache.camel.Component;
import org.apache.camel.Exchange;
import org.apache.camel.Producer;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.spi.Synchronization;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

//TODO Try having unit tests talk to a VM local AMQP broker (like a Qpid broker)
public class SpringAMQPProducerTest extends CamelTestSupport {
    
    @Test
    public void testCreateContext() throws Exception {
        Component component = context().getComponent("spring-amqp", SpringAMQPComponent.class);
        Assert.assertNotNull(component);
    }
    
    @Test 
    public void restartProducer() throws Exception {
        Producer producer = context().getEndpoint("spring-amqp:myExchange:test.z?durable=false&autodelete=true&exclusive=false").createProducer();
        producer.start();
        producer.stop();
    }
    
    @Test
    public void sendMessage() throws Exception {
        context().createProducerTemplate().sendBody("direct:test.z", "HELLO WORLD");
    }
    
    @Test
    public void sendAsyncMessage() throws Exception {
        context().createProducerTemplate().asyncRequestBody("direct:test.x", "HELLO WORLD");
    }
    
    @Test
    public void sendAsyncCallbackMessage() throws Exception {
        context().createProducerTemplate().asyncCallbackSendBody("direct:test.y", "HELLO WORLD", new Synchronization() {
            @Override
            public void onComplete(Exchange exchange) {
                Assert.assertNull(exchange.getException());
            }

            @Override
            public void onFailure(Exchange exchange) {
                Assert.fail(exchange.getException() != null ? exchange.getException().getMessage() : "Failure on async callback");
            }
        });
    }
    
    @Test
    public void sendObject() throws Exception {
        context().createProducerTemplate().sendBody("direct:test.z", new ProducerTestObject());
    }
    
    @Test
    public void sendNull() throws Exception {
        context().createProducerTemplate().sendBody("direct:test.z", null);
    }
    
    @Test
    public void sendUsingDefaultExchange() throws Exception {
        context().createProducerTemplate().sendBody("direct:test.y", null);
    }
    
    @Override
    protected CamelContext createCamelContext() throws Exception {
        CachingConnectionFactory factory = new CachingConnectionFactory();
        RabbitTemplate amqpTemplate = new RabbitTemplate(factory);
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
            	from("direct:test.y").to("spring-amqp::test.y?durable=false&autodelete=true&exclusive=false");
                from("direct:test.z").to("spring-amqp:myExchange:test.z?durable=false&autodelete=true&exclusive=false");
                from("direct:test.x").to("spring-amqp:myExchange:test.x?durable=false&autodelete=true&exclusive=false");
                from("direct:test.y").to("spring-amqp:myExchange:test.y?durable=false&autodelete=true&exclusive=false");
            }
        };
    }
    
    public static class ProducerTestObject implements Serializable {
        private static final long serialVersionUID = -9121162751092118857L;
        private String test;

        public String getTest() {
            return test;
        }

        public void setTest(String test) {
            this.test = test;
        }
    }
}
