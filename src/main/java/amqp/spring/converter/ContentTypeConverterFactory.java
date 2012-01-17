/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */
package amqp.spring.converter;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.AbstractMessageConverter;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;

/**
 * This Spring AMQP MessageConverter does not actually perform the conversion - 
 * instead it find an appropriate converter given the content type
 * within the message's properties.
 */
public class ContentTypeConverterFactory extends AbstractMessageConverter {
    private static transient final Logger LOG = LoggerFactory.getLogger(ContentTypeConverterFactory.class);
    
    protected Map<String, MessageConverter> converters;
    protected String defaultContentType = MessageProperties.CONTENT_TYPE_JSON;
    protected MessageConverter fallbackConverter = null;

    public ContentTypeConverterFactory() {
        this.converters = new HashMap<String, MessageConverter>();
    }
    
    public Map<String, MessageConverter> getConverters() {
        return converters;
    }

    public void setConverters(Map<String, MessageConverter> converters) {
        this.converters = converters;
    }

    public String getDefaultContentType() {
        return defaultContentType;
    }

    public void setDefaultContentType(String defaultContentType) {
        this.defaultContentType = defaultContentType;
    }

    public MessageConverter getFallbackConverter() {
        return fallbackConverter;
    }

    public void setFallbackConverter(MessageConverter fallbackConverter) {
        this.fallbackConverter = fallbackConverter;
    }

    @Override
    protected Message createMessage(Object object, MessageProperties messageProperties) {
        String contentType = messageProperties.getContentType();
        MessageConverter converter = converters.get(contentType);
        if(converter == null) //Try to fall back
            converter = this.fallbackConverter;
        if(converter == null) //Can't even fall back, punt
            throw new MessageConversionException("Cannot find converter for content type of "+contentType);
        
        return converter.toMessage(object, messageProperties);
    }

    @Override
    public Object fromMessage(Message message) throws MessageConversionException {
        MessageProperties messageProperties = message.getMessageProperties();
        String contentType = messageProperties.getContentType();
        if(messageProperties == null)
            throw new MessageConversionException("Cannot decode a message with no properties!");
        
        MessageConverter converter = converters.get(contentType);
        if(converter == null) //Try to fall back
            converter = this.fallbackConverter;
        if(converter == null) //Can't even fall back, punt
            throw new MessageConversionException("Cannot find converter for content type of "+contentType);
        
        return converter.fromMessage(message);
    }
}
