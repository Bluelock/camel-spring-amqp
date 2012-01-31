/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */
package amqp.spring.converter;

import java.io.UnsupportedEncodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.AbstractMessageConverter;
import org.springframework.amqp.support.converter.MessageConversionException;

/**
 * Marshal (and un-marshal) a message into a String given the message's encoding
 */
public class StringConverter extends AbstractMessageConverter {
    private static transient final Logger LOG = LoggerFactory.getLogger(StringConverter.class);
    public static final String DEFAULT_CONTENT_TYPE = "application/text";
    
    protected String encoding = "UTF-8";
    protected String contentType = null;

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
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
                body = object.toString().getBytes(this.encoding);
            }
            
            String msgContentType = this.contentType == null ? DEFAULT_CONTENT_TYPE : this.contentType;
            messageProperties.setContentType(msgContentType);
            messageProperties.setContentEncoding(this.encoding);
            messageProperties.setContentLength(body.length);
            return new Message(body, messageProperties);
        } catch (UnsupportedEncodingException ex) {
            LOG.error("Cannot encode strings as {}", this.encoding, ex);
            throw new MessageConversionException("Cannot encode strings as "+this.encoding, ex);
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
            messageEncoding = this.encoding;

        String messageContentType = messageProperties.getContentType();
        if(this.contentType != null && ! this.contentType.equalsIgnoreCase(messageContentType))
            throw new MessageConversionException("Cannot understand a message of type "+messageContentType);

        try {
            return new String(body, messageEncoding);
        } catch (UnsupportedEncodingException ex) {
            LOG.error("Cannot dencode strings as {}", this.encoding, ex);
            throw new MessageConversionException("Cannot dencode strings as "+this.encoding, ex);
        }
    }
}
