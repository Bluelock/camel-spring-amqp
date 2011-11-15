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
import javax.annotation.Resource;
import org.apache.camel.CamelContext;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Handler;
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
@Component
public class SpringXMLTest {
    @Resource
    protected ProducerTemplate template;
    @Resource
    protected CamelContext camelContext;
    @EndpointInject(uri = "mock:testOne")
    protected MockEndpoint testOne;
    @EndpointInject(uri = "mock:testTwo")
    protected MockEndpoint testTwo;
    
    @Before
    public void resetEndpoints() throws Exception {
        testOne.reset();
        testTwo.reset();
    }
    
    @Test
    public void testHappyPath() throws Exception {
        testOne.expectedMessageCount(1);
        testOne.expectedBodiesReceived("HELLO WORLD");
        template.sendBody("direct:stepOne", "HELLO WORLD");
        testOne.assertIsSatisfied();
    }
    
    @Test
    public void testHeadersExchange() throws Exception {
        testTwo.expectedMessageCount(1);
        testTwo.expectedBodiesReceived("HELLO HEADERS");
        
        Map<String, Object> headers = new HashMap<String, Object>();
        headers.put("key1", "value1");
        headers.put("key2", "value2");
        
        template.sendBodyAndHeaders("direct:stepTwo", "HELLO HEADERS", headers);
        testTwo.assertIsSatisfied();
    }
    
    @Test
    public void testRequestReply() throws Exception {
        String response = template.requestBody("direct:stepThree", "REQUEST", String.class);
        Assert.assertEquals("RESPONSE", response);
    }
    
    @Handler
    public void handle(Exchange exchange) {
        exchange.getOut().setBody("RESPONSE");
    }
}
