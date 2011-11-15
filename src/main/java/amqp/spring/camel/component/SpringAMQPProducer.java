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

import java.util.Map.Entry;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;

public class SpringAMQPProducer extends DefaultProducer {
    private static transient final Logger LOG = LoggerFactory.getLogger(SpringAMQPProducer.class);
    
    protected SpringAMQPEndpoint endpoint;
    private org.springframework.amqp.core.Exchange exchange;
    
    public SpringAMQPProducer(SpringAMQPEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        LOG.trace("Sending exchange {}", exchange.getExchangeId());
        Object body = exchange.getIn().getBody();
        
        if(body == null) {
            LOG.warn("Exchange {} had a null body, creating an empty byte array", exchange.getExchangeId());
            body = new byte[] {};
        }
        
        MessagePostProcessor headerProcessor = new HeadersPostProcessor(exchange.getIn());
        this.endpoint.getAmqpTemplate().convertAndSend(this.endpoint.exchangeName, this.endpoint.routingKey, body, headerProcessor);
    }

    @Override
    public void start() throws Exception {
        super.start();
        
        this.exchange = this.endpoint.createAMQPExchange();
        this.endpoint.amqpAdministration.declareExchange(this.exchange);
        LOG.info("Declared exchange {}", this.exchange.getName());
    }

    @Override
    public void stop() throws Exception {
        if(this.endpoint.amqpAdministration != null && this.exchange != null) {
            this.endpoint.amqpAdministration.deleteExchange(this.exchange.getName());
            LOG.info("Deleted exchange {}", this.exchange.getName());
        }
        
        super.stop();
    }
    
    private static class HeadersPostProcessor implements MessagePostProcessor {
        public org.apache.camel.Message camelMessage;
        
        public HeadersPostProcessor(org.apache.camel.Message camelMessage) {
            this.camelMessage = camelMessage;
        }
        
        @Override
        public Message postProcessMessage(Message msg) throws AmqpException {
            if(camelMessage == null || camelMessage.getHeaders() == null)
                return msg;
                        
            for(Entry<String, Object> headerEntry : camelMessage.getHeaders().entrySet())
                msg.getMessageProperties().setHeader(headerEntry.getKey(), headerEntry.getValue());
            
            return msg;
        }
    }
}
