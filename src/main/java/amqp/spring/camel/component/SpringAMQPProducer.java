/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */

package amqp.spring.camel.component;

import java.util.concurrent.RejectedExecutionException;
import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultAsyncProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;

public class SpringAMQPProducer extends DefaultAsyncProducer {
    private static transient final Logger LOG = LoggerFactory.getLogger(SpringAMQPProducer.class);
    
    protected SpringAMQPEndpoint endpoint;
    private org.springframework.amqp.core.Exchange exchange;
    
    public SpringAMQPProducer(SpringAMQPEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
    }


    @Override
    public boolean process(Exchange exchange, AsyncCallback callback) {
        if(! isRunAllowed()) {
            if(exchange.getException() == null)
                exchange.setException(new RejectedExecutionException("SpringAMQPProducer not started yet!"));
            callback.done(true);
            return true;
        }
        
        try {
            process(exchange);
        } catch(Exception e) {
            if(exchange.getException() == null)
                exchange.setException(e);
        } finally {
            callback.done(true);
        }
        
        return false;
    }
    
    @Override
    public void process(Exchange exchange) throws Exception {
        if(! isRunAllowed()) {
            if(exchange.getException() == null)
                exchange.setException(new RejectedExecutionException("SpringAMQPProducer not started yet!"));
        }
        
        org.apache.camel.Message message = exchange.getIn();
        SpringAMQPMessage inMessage = new SpringAMQPMessage(message);
        exchange.setIn(inMessage); //Swap out the old message format
        
        MessageConverter msgConverter;
        if(this.endpoint.getAmqpTemplate() instanceof RabbitTemplate) {
            RabbitTemplate rabbitTemplate = (RabbitTemplate) this.endpoint.getAmqpTemplate();
            msgConverter = rabbitTemplate.getMessageConverter();
        } else {
            LOG.warn("Cannot find RabbitMQ AMQP Template, falling back to simple message converter");
            msgConverter = new SimpleMessageConverter();
        }
        
        if(exchange.getPattern().isOutCapable()) {
            LOG.debug("Synchronous send and request for exchange {}", exchange.getExchangeId());
            Message amqpResponse = this.endpoint.getAmqpTemplate().sendAndReceive(this.endpoint.exchangeName, this.endpoint.routingKey, inMessage.toAMQPMessage(msgConverter));
            SpringAMQPMessage camelResponse = SpringAMQPMessage.fromAMQPMessage(msgConverter, amqpResponse);
            exchange.setOut(camelResponse);
        } else {
            LOG.debug("Synchronous send for exchange {}", exchange.getExchangeId());
            this.endpoint.getAmqpTemplate().send(this.endpoint.exchangeName, this.endpoint.routingKey, inMessage.toAMQPMessage(msgConverter));
        }
    }

    @Override
    public void start() throws Exception {
        super.start();
        
        this.exchange = this.endpoint.createAMQPExchange();
        this.endpoint.amqpAdministration.declareExchange(this.exchange);
        LOG.info("Declared exchange {}", this.exchange.getName());
    }
}
