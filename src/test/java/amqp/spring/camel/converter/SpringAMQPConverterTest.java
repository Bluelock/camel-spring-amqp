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
package amqp.spring.camel.converter;

import amqp.spring.camel.component.SpringAMQPMessage;
import org.apache.camel.impl.DefaultMessage;
import org.junit.Assert;
import org.junit.Test;

public class SpringAMQPConverterTest {
    @Test
    public void testConversion() throws Exception {
        DefaultMessage oldMessage = new DefaultMessage();
        oldMessage.setBody("Test String");
        oldMessage.setHeader("Old Header", "Old Header Value");
        
        SpringAMQPMessage newMessage = SpringAMQPConverter.convertFrom(oldMessage, null);
        Assert.assertEquals("Test String", newMessage.getBody(String.class));
        Assert.assertEquals("Old Header Value", newMessage.getHeader("Old Header"));
    }
}
