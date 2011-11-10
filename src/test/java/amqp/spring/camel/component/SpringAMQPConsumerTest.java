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
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

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
        
        Consumer amqpConsumer = context().getEndpoint("spring-amqp:myExchange:myQueue:test.b?durable=false&autodelete=true&exclusive=false").createConsumer(defaultProcessor);
        amqpConsumer.stop();
        amqpConsumer.start();
    }
    
    @Test
    public void sendMessage() throws Exception {
        MockEndpoint mockEndpoint = context().getEndpoint("mock:test.b", MockEndpoint.class);
        mockEndpoint.expectedMessageCount(1);
        context().createProducerTemplate().sendBody("spring-amqp:myExchange:test.b?durable=false&autodelete=true&exclusive=false", "HELLO WORLD");
        mockEndpoint.assertIsSatisfied();
    }

    @Override
    protected CamelContext createCamelContext() throws Exception {
        ConnectionFactory factory = new CachingConnectionFactory();
        
        CamelContext camelContext = super.createCamelContext();
        camelContext.addComponent("spring-amqp", new SpringAMQPComponent(factory));
        return camelContext;
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("spring-amqp:myExchange:myQueue:test.b?durable=false&autodelete=true&exclusive=false").to("mock:test.b");
            }
        };
    }
}
