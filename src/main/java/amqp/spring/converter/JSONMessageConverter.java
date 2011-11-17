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
package amqp.spring.converter;

import java.io.IOException;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.AbstractMessageConverter;
import org.springframework.amqp.support.converter.ClassMapper;
import org.springframework.amqp.support.converter.DefaultClassMapper;
import org.springframework.amqp.support.converter.MessageConversionException;

public class JSONMessageConverter extends AbstractMessageConverter {
    private static transient final Logger LOG = LoggerFactory.getLogger(JSONMessageConverter.class);
    
    protected ObjectMapper objectMapper;
    protected ClassMapper classMapper;
    protected String encoding = "UTF-8";
    
    public JSONMessageConverter() {
        this.classMapper = new DefaultClassMapper();
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    @Override
    protected Message createMessage(Object object, MessageProperties messageProperties) {
        try {
            byte[] body = null;
            if(object != null) {
                String json = this.objectMapper.writeValueAsString(object);
                body = json.getBytes(this.encoding);
            }
            
            messageProperties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
            messageProperties.setContentEncoding(this.encoding);
            messageProperties.setContentLength(body.length);
            classMapper.fromClass(object.getClass(), messageProperties);
            return new Message(body, messageProperties);
        } catch (JsonGenerationException ex) {
            LOG.error("JsonGenerationException trying to marshal message of type {}", object.getClass().getCanonicalName(), ex);
            throw new MessageConversionException("Could not marshal message of type "+object.getClass().getCanonicalName(), ex);
        } catch (JsonMappingException ex) {
            LOG.error("JsonMappingException trying to jarshal message of type {}", object.getClass().getCanonicalName(), ex);
            throw new MessageConversionException("Could not marshal message of type "+object.getClass().getCanonicalName(), ex);
        } catch (IOException ex) {
            LOG.error("IOException trying to marshal message of type {}", object.getClass().getCanonicalName(), ex);
            throw new MessageConversionException("Could not marshal message of type "+object.getClass().getCanonicalName(), ex);
        }
    }

    @Override
    public Object fromMessage(Message message) throws MessageConversionException {
        MessageProperties messageProperties = message.getMessageProperties();
        if(messageProperties == null)
            throw new MessageConversionException("Cannot decode a message with no properties!");

        byte[] body = message.getBody();
        if(body == null)
            return null;

        String messageEncoding = messageProperties.getContentEncoding();
        if(messageEncoding == null)
            messageEncoding = getEncoding();

        String contentType = messageProperties.getContentType();
        if(! MessageProperties.CONTENT_TYPE_JSON.equalsIgnoreCase(contentType))
            throw new MessageConversionException("Cannot understand a message of type "+contentType);

        Class<?> bodyClass = this.classMapper.toClass(messageProperties);
            
        try {
            String json = new String(body, messageEncoding);
            return this.objectMapper.readValue(json, bodyClass);
        } catch (JsonParseException ex) {
            LOG.error("JsonParseException trying to unmarshal message of type {}", bodyClass.getCanonicalName(), ex);
            throw new MessageConversionException("Could not unmarshal message of type "+bodyClass.getCanonicalName(), ex);
        } catch (JsonMappingException ex) {
            LOG.error("JsonMappingException trying to unmarshal message of type {}", bodyClass.getCanonicalName(), ex);
            throw new MessageConversionException("Could not unmarshal message of type "+bodyClass.getCanonicalName(), ex);
        } catch (IOException ex) {
            LOG.error("IOException trying to unmarshal message of type {}", bodyClass.getCanonicalName(), ex);
            throw new MessageConversionException("Could not unmarshal message of type "+bodyClass.getCanonicalName(), ex);
        }
    }
}
