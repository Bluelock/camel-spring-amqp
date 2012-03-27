/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */

package amqp.spring.camel.component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultAsyncProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;

public class SpringAMQPProducer extends DefaultAsyncProducer implements ConnectionListener {
    private static transient final Logger LOG = LoggerFactory.getLogger(SpringAMQPProducer.class);
    
    protected SpringAMQPEndpoint endpoint;
    private org.springframework.amqp.core.Exchange exchange;
    private ExecutorService threadPool;
    private boolean failed;
    
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
        
        if(this.threadPool == null) {
            if(exchange.getException() == null)
                exchange.setException(new RejectedExecutionException("SpringAMQPProducer is not yet initialized!"));
            callback.done(true);
            return true;
        }
        
        this.threadPool.execute(new AMQPProducerTask(exchange, callback));
        return false;
    }
    
    @Override
    public void process(Exchange exchange) throws Exception {
        if(! isRunAllowed()) {
            if(exchange.getException() == null)
                exchange.setException(new RejectedExecutionException("SpringAMQPProducer not started yet!"));
        }
        
        //This is an intentional synchronous invocation of run(), don't mock me
        new AMQPProducerTask(exchange).run();
    }

    @Override
    public void doStart() throws Exception {
        super.doStart();
        
        this.exchange = this.endpoint.createAMQPExchange();
        if (this.endpoint.isUsingDefaultExchange()) {
            LOG.info("Using default exchange of type {}", this.exchange.getClass().getSimpleName());
        } else {
            LOG.info("Declaring exchange {} of type {}", this.exchange.getName(), this.exchange.getClass().getSimpleName());
            try {
                this.endpoint.amqpAdministration.declareExchange(this.exchange);
            } catch(AmqpIOException e) {
                //The actual reason for failed exceptions is often swallowed up by Camel or Spring, find it
                Throwable rootCause = SpringAMQPComponent.findRootCause(e);
                LOG.error("Could not initialize exchange!", rootCause);
                throw e;
            } catch (AmqpConnectException e) {
                LOG.error("Producer cannot connect to broker - stopping endpoint {}", this.endpoint.toString(), e);
                stop();
                this.endpoint.stop();
                return;
            }
        }

        //Initialize execution pool
        //TODO In Camel 2.9+ this becomes ExecutorServiceManager
        this.threadPool = this.endpoint.getCamelContext().getExecutorServiceStrategy().newDefaultThreadPool(this, "amqp-producer");
    }

    @Override
    public void doShutdown() throws Exception {
        super.doShutdown();
        
        if(this.threadPool != null) {
            this.threadPool.shutdownNow();
        }
    }
    
    @Override
    public void doStop() throws Exception {
        super.doStop();
        
        if(this.threadPool != null) {
            this.threadPool.shutdownNow();
        }
    }

    @Override
    public void onCreate(Connection connection) {
        if(! this.failed) return; //We're not recovering from a failure, nevermind
        
        LOG.warn("Noticed that the broker has come online, attempting to re-start producer {}", this.getEndpoint().getEndpointUri());
        try {
            doStart();
            this.failed = false;
        } catch(Exception e) {
            LOG.error("Could not re-start producer {}", this.getEndpoint().getEndpointUri(), e);
        }
    }

    @Override
    public void onClose(Connection connection) {
        this.failed = true;
        LOG.warn("Noticed that the broker has gone offline, attempting to stop producer {}", this.getEndpoint().getEndpointUri());
        try {
            doStop();
        } catch(Exception e) {
            LOG.error("Could not stop producer {}", this.getEndpoint().getEndpointUri(), e);
        }
    }
    
    private class AMQPProducerTask implements Runnable {
        Exchange exchange;
        AsyncCallback callback;
        
        public AMQPProducerTask(Exchange exchange) {
            this(exchange, null);
        }
        
        public AMQPProducerTask(Exchange exchange, AsyncCallback callback) {
            this.exchange = exchange;
            this.callback = callback;
        }
        
        @Override
        public void run() {
            org.apache.camel.Message message = exchange.getIn();
            SpringAMQPMessage inMessage = new SpringAMQPMessage(message);
            exchange.setIn(inMessage); //Swap out the old message format

            MessageConverter msgConverter;
            if(endpoint.getAmqpTemplate() instanceof RabbitTemplate) {
                RabbitTemplate rabbitTemplate = (RabbitTemplate) endpoint.getAmqpTemplate();
                msgConverter = rabbitTemplate.getMessageConverter();
            } else {
                LOG.warn("Cannot find RabbitMQ AMQP Template, falling back to simple message converter");
                msgConverter = new SimpleMessageConverter();
            }
            
            String routingKeyHeader = message.getHeader(SpringAMQPComponent.ROUTING_KEY_HEADER, String.class);
            String routingKey = routingKeyHeader != null ? routingKeyHeader : endpoint.routingKey;

            try {
                if(exchange.getPattern().isOutCapable()) {
                    LOG.debug("Synchronous send and request for exchange {}", exchange.getExchangeId());
                    Message amqpResponse = endpoint.getAmqpTemplate().sendAndReceive(endpoint.exchangeName, routingKey, inMessage.toAMQPMessage(msgConverter));
                    SpringAMQPMessage camelResponse = SpringAMQPMessage.fromAMQPMessage(msgConverter, amqpResponse);
                    exchange.setOut(camelResponse);
                } else {
                    LOG.debug("Synchronous send for exchange {}", exchange.getExchangeId());
                    endpoint.getAmqpTemplate().send(endpoint.exchangeName, routingKey, inMessage.toAMQPMessage(msgConverter));
                }
            } catch (AmqpConnectException e) {
                LOG.error("AMQP Connection error, marking this connection as failed", e);
                onClose(null);
            } catch (Throwable t) {
                LOG.error("Could not deliver message via AMQP", t);
            }
            
            if(callback != null) 
                callback.done(false);
        }
    }
}
