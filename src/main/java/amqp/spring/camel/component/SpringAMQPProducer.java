/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */

package amqp.spring.camel.component;

import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultAsyncProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

public class SpringAMQPProducer extends DefaultAsyncProducer {
    private static transient final Logger LOG = LoggerFactory.getLogger(SpringAMQPProducer.class);
    
    protected SpringAMQPEndpoint endpoint;
    private org.springframework.amqp.core.Exchange exchange;
    private ExecutorService threadPool;
    
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
        
        this.threadPool.submit(new AMQPProducerTask(exchange, callback));
        return false;
    }
    
    @Override
    public void process(Exchange exchange) throws Exception {
        if(! isRunAllowed() && exchange.getException() == null) {
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
        this.threadPool = this.endpoint.getCamelContext().getExecutorServiceManager().newDefaultThreadPool(this, "amqp-producer");
    }

    @Override
    public void doShutdown() throws Exception {
        super.doShutdown();
        
        if(this.threadPool != null) {
            this.threadPool.shutdown();
            this.threadPool = null;
        }
    }
    
    @Override
    public void doStop() throws Exception {
        super.doStop();
        
        if(this.threadPool != null) {
            this.threadPool.shutdown();
            this.threadPool = null;
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
            
            String exchangeNameHeader = message.getHeader(SpringAMQPComponent.EXCHANGE_NAME_HEADER, String.class);
            String exchangeName = exchangeNameHeader != null ? exchangeNameHeader : endpoint.exchangeName;

            try {
                if(exchange.getPattern().isOutCapable()) {
                    LOG.debug("Synchronous send and request for exchange {}", exchange.getExchangeId());
                    Message amqpResponse = endpoint.getAmqpTemplate().sendAndReceive(exchangeName, routingKey, inMessage.toAMQPMessage(msgConverter));
                    SpringAMQPMessage camelResponse = SpringAMQPMessage.fromAMQPMessage(msgConverter, amqpResponse);

                    Boolean isExceptionCaught = (Boolean)camelResponse.getHeader(SpringAMQPMessage.IS_EXCEPTION_CAUGHT);
                    if (isExceptionCaught != null && isExceptionCaught.equals(Boolean.TRUE)) {
                        Object caughtObject = camelResponse.getBody();
                        if (caughtObject == null) {
                            exchange.setException(new RuntimeException("Null exception caught from Camel."));
                        } else if (caughtObject instanceof Throwable) {
                            exchange.setException((Throwable)caughtObject);
                        } else {
                            exchange.setException(new RuntimeException(caughtObject.toString()));
                        }
                    } else {
                        exchange.setOut(camelResponse);
                    }
                } else {
                    LOG.debug("Synchronous send for exchange {}", exchange.getExchangeId());
                    endpoint.getAmqpTemplate().send(exchangeName, routingKey, inMessage.toAMQPMessage(msgConverter));
                }
            } catch (Throwable t) {
                LOG.error("Could not deliver message via AMQP", t);
            }
            
            if(callback != null) 
                callback.done(false);
        }
    }
}
