/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */

package amqp.spring.camel.component;

import java.util.concurrent.*;
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
    private ExecutorService threadPool;
    
    public SpringAMQPProducer(SpringAMQPEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
        
        BlockingQueue threadQueue = new LinkedBlockingQueue(endpoint.getThreadPoolMaxSize());
        this.threadPool = new ThreadPoolExecutor(endpoint.getThreadPoolIdleSize(), endpoint.getThreadPoolMaxSize(), 
                endpoint.getIdleThreadKeepAliveMillis(), TimeUnit.MILLISECONDS, threadQueue);
    }

    @Override
    public boolean process(Exchange exchange, AsyncCallback callback) {
        if(! isRunAllowed()) {
            if(exchange.getException() == null)
                exchange.setException(new RejectedExecutionException("SpringAMQPProducer not started yet!"));
            callback.done(true);
            return true;
        }
        
        assert this.threadPool != null;
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
    public void start() throws Exception {
        super.start();
        
        this.exchange = this.endpoint.createAMQPExchange();
        this.endpoint.amqpAdministration.declareExchange(this.exchange);
        LOG.info("Declared exchange {}", this.exchange.getName());
    }

    @Override
    public void shutdown() throws Exception {
        super.shutdown();
        
        this.threadPool.shutdown();
    }
    
    @Override
    public void stop() throws Exception {
        super.stop();
        
        this.threadPool.shutdownNow();
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

            if(exchange.getPattern().isOutCapable()) {
                LOG.debug("Synchronous send and request for exchange {}", exchange.getExchangeId());
                Message amqpResponse = endpoint.getAmqpTemplate().sendAndReceive(endpoint.exchangeName, endpoint.routingKey, inMessage.toAMQPMessage(msgConverter));
                SpringAMQPMessage camelResponse = SpringAMQPMessage.fromAMQPMessage(msgConverter, amqpResponse);
                exchange.setOut(camelResponse);
            } else {
                LOG.debug("Synchronous send for exchange {}", exchange.getExchangeId());
                endpoint.getAmqpTemplate().send(endpoint.exchangeName, endpoint.routingKey, inMessage.toAMQPMessage(msgConverter));
            }
            
            if(callback != null) 
                callback.done(false);
        }
    }
}
