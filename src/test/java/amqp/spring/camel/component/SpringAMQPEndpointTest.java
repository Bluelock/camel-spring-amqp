/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */
package amqp.spring.camel.component;

import junit.framework.Assert;
import org.apache.camel.CamelContext;
import org.apache.camel.Component;
import org.apache.camel.Endpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

public class SpringAMQPEndpointTest extends CamelTestSupport {
    
    @Test
    public void testCreateContext() throws Exception {
        Component component = context().getComponent("spring-amqp", SpringAMQPComponent.class);
        Assert.assertNotNull(component);
        
        Endpoint endpoint = component.createEndpoint("spring-amqp:test.a");
        Assert.assertNotNull(endpoint);
    }
    
    @Override
    protected CamelContext createCamelContext() throws Exception {
        ConnectionFactory factory = new CachingConnectionFactory();
        
        CamelContext camelContext = super.createCamelContext();
        camelContext.addComponent("spring-amqp", new SpringAMQPComponent(factory));
        return camelContext;
    }
}
