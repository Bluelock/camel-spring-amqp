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
import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultAsyncProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;

public class SpringAMQPProducer extends DefaultAsyncProducer {
    private static transient final Logger LOG = LoggerFactory.getLogger(SpringAMQPProducer.class);
    
    protected SpringAMQPEndpoint endpoint;
    private org.springframework.amqp.core.Exchange exchange;
    
    public SpringAMQPProducer(SpringAMQPEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Object body = exchange.getIn().getBody();
        
        if(body == null) {
            LOG.warn("Exchange {} had a null body, creating an empty byte array", exchange.getExchangeId());
            body = new byte[] {};
        }
        
        MessagePostProcessor headerProcessor = new HeadersPostProcessor(exchange.getIn());
        
        if(exchange.getPattern().isOutCapable()) {
            LOG.debug("Synchronous send and request for exchange {}", exchange.getExchangeId());
            Object response = this.endpoint.getAmqpTemplate().convertSendAndReceive(this.endpoint.exchangeName, this.endpoint.routingKey, body, headerProcessor);
            exchange.getOut().copyFrom(exchange.getIn());
            exchange.getOut().setBody(response);
        } else {
            LOG.debug("Synchronous send for exchange {}", exchange.getExchangeId());
            this.endpoint.getAmqpTemplate().convertAndSend(this.endpoint.exchangeName, this.endpoint.routingKey, body, headerProcessor);
        }
    }

    @Override
    public boolean process(Exchange exchange, AsyncCallback callback) {
        if(exchange.getPattern().isOutCapable()) {
            LOG.debug("Asynchronous send and request for exchange {}", exchange.getExchangeId());
            new Thread(new AsyncResponseTask(exchange, callback)).start();
            return false;
        } else {
            try {
                process(exchange);
            } catch(Exception e) {
                exchange.setException(e);
            } finally {
                callback.done(true);
            }
        
            return true;
        }
    }

    @Override
    public void start() throws Exception {
        super.start();
        
        this.exchange = this.endpoint.createAMQPExchange();
        this.endpoint.amqpAdministration.declareExchange(this.exchange);
        LOG.info("Declared exchange {}", this.exchange.getName());
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
    
    private class AsyncResponseTask implements Runnable {
        Exchange exchange;
        AsyncCallback callback;
        
        public AsyncResponseTask(Exchange exchange, AsyncCallback callback) {
            this.exchange = exchange;
            this.callback = callback;
        }

        @Override
        public void run() {
            MessagePostProcessor headerProcessor = new HeadersPostProcessor(this.exchange.getIn());
            Object body = this.exchange.getIn().getBody();
            if(body == null) {
                LOG.warn("Exchange {} had a null body, creating an empty byte array", this.exchange.getExchangeId());
                body = new byte[] {};
            }
        
            exchange.getOut().copyFrom(exchange.getIn());
            try {
                Object response = endpoint.getAmqpTemplate().convertSendAndReceive(endpoint.getExchangeName(), endpoint.getRoutingKey(), body, headerProcessor);
                exchange.getOut().setBody(response);
            } catch (Exception e) {
                this.exchange.setException(e);
            } finally {
                callback.done(false);
            }
        }
    }
}
