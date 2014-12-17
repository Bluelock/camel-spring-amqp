/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */
package amqp.spring.camel.component;

import org.apache.camel.CamelContext;
import org.apache.camel.Component;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

public class SpringAMQPComponentTest extends CamelTestSupport {
    
    @Test
    public void testCreateContext() throws Exception {
        Component component = context().getComponent("spring-amqp", SpringAMQPComponent.class);
        Assert.assertNotNull(component);
    }
    
    @Test
    public void testFindRootCause() throws Exception {
        IllegalStateException child = new IllegalStateException("Child Exception");
        RuntimeException parent = new RuntimeException("Parent Exception", child);
        RuntimeException grandparent = new RuntimeException("Grandparent Exception", parent);
        Assert.assertEquals(child, SpringAMQPComponent.findRootCause(grandparent));
    }
    
    @Override
    protected CamelContext createCamelContext() throws Exception {
        ConnectionFactory factory = new CachingConnectionFactory();
        
        CamelContext camelContext = super.createCamelContext();
        camelContext.addComponent("spring-amqp", new SpringAMQPComponent(factory));
        return camelContext;
    }
}
