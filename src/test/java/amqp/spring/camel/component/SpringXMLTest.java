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

import javax.annotation.Resource;
import org.apache.camel.CamelContext;
import org.apache.camel.EndpointInject;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.mock.MockEndpoint;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

//TODO Try having unit tests talk to a VM local AMQP broker (like a Qpid broker)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@Component("testSpringXML")
public class SpringXMLTest {
    @Resource
    protected ProducerTemplate template;
    @Resource
    protected CamelContext camelContext;
    @EndpointInject(uri = "mock:testOne")
    protected MockEndpoint testOne;
    
    @Before
    public void resetEndpoints() throws Exception {
        testOne.reset();
    }
    
    @Test
    public void testHappyPath() throws Exception {
        testOne.expectedMessageCount(1);
        testOne.expectedBodiesReceived("HELLO WORLD");
        Object response = template.requestBody("direct:stepOne", "HELLO WORLD");
        testOne.assertIsSatisfied();
        Assert.assertNotNull(response);
    }
}
