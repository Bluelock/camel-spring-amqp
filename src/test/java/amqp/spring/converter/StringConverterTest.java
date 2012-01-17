/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */
package amqp.spring.converter;

import java.io.Serializable;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConverter;

public class StringConverterTest {
    
    @Test
    public void testConversion() throws Exception {
        TestObject testObject = new TestObject();
        testObject.setValue("TESTING");
        
        MessageProperties messageProperties = new MessageProperties();
        
        MessageConverter converter = new StringConverter();
        Message amqpMessage = converter.toMessage(testObject, messageProperties);
        Object newObject = converter.fromMessage(amqpMessage);
        
        Assert.assertEquals("TESTING", newObject);
    }
    
    private static class TestObject implements Serializable {
        private static final long serialVersionUID = 8035548300959603643L;
        protected String value;
        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final TestObject other = (TestObject) obj;
            if ((this.value == null) ? (other.value != null) : !this.value.equals(other.value)) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 11 * hash + (this.value != null ? this.value.hashCode() : 0);
            return hash;
        }
        
        @Override
        public String toString() {
            return this.value;
        }
    }
}
