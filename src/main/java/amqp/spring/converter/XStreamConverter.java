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

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.QNameMap;
import com.thoughtworks.xstream.io.xml.StaxReader;
import com.thoughtworks.xstream.io.xml.StaxWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import javax.xml.stream.XMLStreamException;
import org.codehaus.jettison.mapped.MappedXMLInputFactory;
import org.codehaus.jettison.mapped.MappedXMLOutputFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.AbstractMessageConverter;
import org.springframework.amqp.support.converter.ClassMapper;
import org.springframework.amqp.support.converter.DefaultClassMapper;
import org.springframework.amqp.support.converter.MessageConversionException;

/**
 * Marshal (and un-marshal) a message into JSON using XStream
 */
public class XStreamConverter extends AbstractMessageConverter {
    private static transient final Logger LOG = LoggerFactory.getLogger(XStreamConverter.class);
    
    protected String encoding = "UTF-8";
    protected ClassMapper classMapper;
    protected XStream objectMapper;
    protected MappedXMLOutputFactory outputFactory;
    protected MappedXMLInputFactory inputFactory;

    public XStreamConverter() {
        this.classMapper = new DefaultClassMapper();
        
 	Map nstjsons = new HashMap();
        this.outputFactory = new MappedXMLOutputFactory(nstjsons);
        this.inputFactory = new MappedXMLInputFactory(nstjsons);
        this.objectMapper = new XStream();
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
                ByteArrayOutputStream outStream = new ByteArrayOutputStream();
                StaxWriter writer = new StaxWriter(new QNameMap(), this.outputFactory.createXMLStreamWriter(outStream));
                this.objectMapper.marshal(object, writer);
                body = outStream.toByteArray();
            }
            
            messageProperties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
            messageProperties.setContentEncoding(this.encoding);
            messageProperties.setContentLength(body.length);
            classMapper.fromClass(object.getClass(), messageProperties);
            return new Message(body, messageProperties);
        } catch (XMLStreamException ex) {
            String typeId = (String) messageProperties.getHeaders().get(DefaultClassMapper.DEFAULT_CLASSID_FIELD_NAME);
            LOG.error("XMLStreamException trying to marshal message of type {}", typeId, ex);
            throw new MessageConversionException("Could not marshal message of type "+typeId, ex);
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

        try {
            ByteArrayInputStream inStream = new ByteArrayInputStream(body);
            StaxReader reader = new StaxReader(new QNameMap(), this.inputFactory.createXMLStreamReader(inStream, getEncoding()));
            return this.objectMapper.unmarshal(reader);
        } catch (XMLStreamException ex) {
            String typeId = (String) messageProperties.getHeaders().get(DefaultClassMapper.DEFAULT_CLASSID_FIELD_NAME);
            LOG.error("XMLStreamException trying to unmarshal message of type {}", typeId, ex);
            throw new MessageConversionException("Could not unmarshal message of type "+typeId, ex);
        }
    }
}
