/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */

package amqp.spring.camel.component;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicBoolean;
import org.aopalliance.aop.Advice;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.camel.impl.DefaultExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.StatefulRetryOperationsInterceptorFactoryBean;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.retry.MessageKeyGenerator;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.ErrorHandler;

public class SpringAMQPConsumer extends DefaultConsumer implements ConnectionListener {
    private static transient final Logger LOG = LoggerFactory.getLogger(SpringAMQPConsumer.class);
    private static final String TTL_QUEUE_ARGUMENT = "x-message-ttl";
    private static final String HA_POLICY_ARGUMENT = "x-ha-policy";
    
    protected SpringAMQPEndpoint endpoint;
    private RabbitMQConsumerTask messageListener;
    private Queue queue;
    private Binding binding;
    private final AtomicBoolean failed = new AtomicBoolean(false);
    
    public SpringAMQPConsumer(SpringAMQPEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
        this.messageListener = new RabbitMQConsumerTask((RabbitTemplate) this.endpoint.getAmqpTemplate(), 
                this.endpoint.queueName, this.endpoint.getConcurrentConsumers(), this.endpoint.getPrefetchCount());
    }

    @Override
    public void doStart() throws Exception {
        super.doStart();
        
        org.springframework.amqp.core.Exchange exchange = this.endpoint.createAMQPExchange();
        if (this.endpoint.isUsingDefaultExchange()) {
            LOG.info("Using default exchange, will not declare one");
        } else {
            try {
        	this.endpoint.amqpAdministration.declareExchange(exchange);
        	LOG.info("Declared exchange {}", exchange.getName());
            } catch (AmqpIOException e) {
                if(LOG.isDebugEnabled()) {
                    LOG.warn("Could not declare exchange {}, possible re-declaration of a different type?", exchange.getName());
                } else {
                    LOG.warn("Could not declare exchange {}, possible re-declaration of a different type?", exchange.getName(), e);
                }
            } catch (AmqpConnectException e) {
                LOG.error("Consumer cannot connect to broker - stopping endpoint {}", this.endpoint.toString(), e);
                stop();
                this.endpoint.stop();
                return;
            }
        }

        //Determine queue arguments, including vendor extensions
        Map<String, Object> queueArguments = new HashMap<String, Object>();
        if(endpoint.getTimeToLive() != null)
            queueArguments.put(TTL_QUEUE_ARGUMENT, endpoint.getTimeToLive());
        if(endpoint.isHa() )
            queueArguments.put(HA_POLICY_ARGUMENT, "all");
        
        //Declare queue
        this.queue = new Queue(this.endpoint.queueName, this.endpoint.durable, this.endpoint.exclusive, this.endpoint.autodelete, queueArguments);
        this.endpoint.getAmqpAdministration().declareQueue(queue);
        LOG.info("Declared queue {}", this.queue.getName());
        
        //Is this a header exchange? Bind the key/value pair(s)
        if(exchange instanceof HeadersExchange) {
            if(this.endpoint.routingKey == null)
                throw new IllegalStateException("Specified a header exchange without a key/value match");
            
            if(this.endpoint.routingKey.contains("|") && this.endpoint.routingKey.contains("&"))
                throw new IllegalArgumentException("You cannot mix AND and OR expressions within a header binding");
        
            Map<String, Object> keyValues = parseKeyValues(this.endpoint.routingKey);
            BindingBuilder.HeadersExchangeMapConfigurer mapConfig = BindingBuilder.bind(this.queue).to((HeadersExchange) exchange);
            if(this.endpoint.routingKey.contains("|"))
                this.binding = mapConfig.whereAny(keyValues).match();
            else
                this.binding = mapConfig.whereAll(keyValues).match();
            
        //Is this a fanout exchange? Just bind the queue and exchange directly
        } else if(exchange instanceof FanoutExchange) { 
            this.binding = BindingBuilder.bind(this.queue).to((FanoutExchange) exchange);
        
        //Perform routing key binding for direct or topic exchanges
        } else {
            this.binding = BindingBuilder.bind(this.queue).to(exchange).with(this.endpoint.routingKey).noargs();
        }
        
        if(this.endpoint.isUsingDefaultExchange()) {
            LOG.info("Default exchange is implicitly bound to every queue, with a routing key equal to the queue name");
        } else if (this.binding != null) {
            LOG.info("Declaring binding {}", this.binding.getRoutingKey());
            this.endpoint.getAmqpAdministration().declareBinding(binding);
        }
        
        if(! this.messageListener.listenerContainer.isActive())
            this.messageListener.start();
    }

    @Override
    public void doShutdown() throws Exception {
        this.messageListener.shutdown();
        super.shutdown();
    }
    
    protected static Map<String, Object> parseKeyValues(String routingKey) {
        StringTokenizer tokenizer = new StringTokenizer(routingKey, "&|");
        Map<String, Object> pairs = new HashMap<String, Object>();
        while(tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            String[] keyValue = token.split("=");
            if(keyValue.length != 2)
                throw new IllegalArgumentException("Couldn't parse key/value pair ["+token+"] out of string: "+routingKey);
            
            pairs.put(keyValue[0], keyValue[1]);
        }
        
        return pairs;
    }

    @Override
    public void onCreate(Connection connection) {
        if(! this.failed.compareAndSet(true, false)) return; //We are not in a failure mode

        LOG.warn("Noticed that the broker has come online, attempting to re-start consumer {}", this.getEndpoint().getEndpointUri());
        try {
            doStart();
        } catch(Exception e) {
            LOG.error("Could not re-start consumer {}", this.getEndpoint().getEndpointUri(), e);
        }
    }

    @Override
    public void onClose(Connection connection) {
        this.failed.set(true);
        LOG.warn("Noticed that the broker has gone offline, attempting to stop consumer {}", this.getEndpoint().getEndpointUri());
        try {
            doStop();
        } catch(Exception e) {
            LOG.error("Could not stop consumer {}", this.getEndpoint().getEndpointUri(), e);
        }
    }
    
    //We have to ask the RabbitMQ Template for converters, the interface doesn't have a way to get MessageConverter
    class RabbitMQConsumerTask implements MessageListener {
        private MessageConverter msgConverter;
        private SimpleMessageListenerContainer listenerContainer;
        private static final long DEFAULT_TIMEOUT_MILLIS = 1000;
        
        public RabbitMQConsumerTask(RabbitTemplate template, String queue, int concurrentConsumers, int prefetchCount) {
            this.listenerContainer = new SimpleMessageListenerContainer();
            
            if(template != null) {
                this.msgConverter = template.getMessageConverter();
                this.listenerContainer.setConnectionFactory(template.getConnectionFactory());
            } else {
                LOG.error("No AMQP Template found! Cannot initialize message conversion or connections!");
            }
            
            this.listenerContainer.setQueueNames(queue);
            this.listenerContainer.setConcurrentConsumers(concurrentConsumers);
            this.listenerContainer.setPrefetchCount(prefetchCount);
            
            //Set error handling (send it to Camel)
            this.listenerContainer.setErrorHandler(getErrorHandler());
            this.listenerContainer.setAdviceChain(getAdviceChain());
            
            //Set timeouts
            this.listenerContainer.setShutdownTimeout(DEFAULT_TIMEOUT_MILLIS);
            this.listenerContainer.setReceiveTimeout(DEFAULT_TIMEOUT_MILLIS);
            this.listenerContainer.setRecoveryInterval(DEFAULT_TIMEOUT_MILLIS / 2);
           
            //Transactions are currently not supported
            this.listenerContainer.setChannelTransacted(false);
            this.listenerContainer.setAcknowledgeMode(AcknowledgeMode.NONE);
        }
        
        public void start() {
            this.listenerContainer.setMessageListener(this);
            this.listenerContainer.start();
            LOG.info("Started AMQP Async Listeners for {}", endpoint.getEndpointUri());
        }
        
        public void stop() {
            this.listenerContainer.setConcurrentConsumers(0);
            this.listenerContainer.setPrefetchCount(0);
            this.listenerContainer.stop();
        }
        
        public void shutdown() {
            this.listenerContainer.shutdown();
            this.listenerContainer.destroy();
        }

        public final ErrorHandler getErrorHandler() {
            return new ErrorHandler() {
                @Override
                public void handleError(Throwable t) {
                    if(t instanceof AmqpConnectException) {
                        LOG.error("AMQP Connection error, marking this connection as failed");
                        onClose(null);
                    }
                    
                    getExceptionHandler().handleException(t);
                }
            };
        }
        
        /**
         * Do not have Spring AMQP re-try messages upon failure, leave it to Camel
         * @return An advice chain populated with a NeverRetryPolicy
         */
        public final Advice[] getAdviceChain() {
            RetryTemplate retryRule = new RetryTemplate();
            retryRule.setRetryPolicy(new NeverRetryPolicy());
            
            StatefulRetryOperationsInterceptorFactoryBean retryOperation = new StatefulRetryOperationsInterceptorFactoryBean();
            retryOperation.setRetryOperations(retryRule);
            retryOperation.setMessageKeyGeneretor(new DefaultKeyGenerator());
            
            return new Advice[] { retryOperation.getObject() };
        }

        @Override
        public void onMessage(Message amqpMessage) {
            if(this.msgConverter == null)
                throw new IllegalStateException("No message converter present - cannot processs messages!");
            
            LOG.debug("Received message for routing key {}", amqpMessage.getMessageProperties().getReceivedRoutingKey());
            ExchangePattern exchangePattern = SpringAMQPMessage.getExchangePattern(amqpMessage);
            Exchange exchange = new DefaultExchange(endpoint, exchangePattern);
            SpringAMQPMessage camelMessage = SpringAMQPMessage.fromAMQPMessage(msgConverter, amqpMessage);
            exchange.setIn(camelMessage);
            
            try {
                getProcessor().process(exchange);
            } catch(Throwable t) {
                exchange.setException(t);
            }
            
            //Send a reply if one was requested
            Address replyToAddress = amqpMessage.getMessageProperties().getReplyToAddress();
            if(replyToAddress != null) {
                org.apache.camel.Message outMessage = exchange.getOut();
                SpringAMQPMessage replyMessage = new SpringAMQPMessage(outMessage);
                exchange.setOut(replyMessage); //Swap out the outbound message

                try {
                    endpoint.getAmqpTemplate().send(replyToAddress.getExchangeName(), replyToAddress.getRoutingKey(), replyMessage.toAMQPMessage(msgConverter));
                } catch(AmqpConnectException e) {
                    LOG.error("AMQP Connection error, marking this connection as failed");
                    onClose(null);
                }
            }
        }
    }

    //If the producer does not generate an ID, let's do so now
    static class DefaultKeyGenerator implements MessageKeyGenerator {
        public static final String ALGORITHM = "MD5";
        
        @Override
        public Object getKey(Message message) {
            try {
                MessageDigest digest = MessageDigest.getInstance(ALGORITHM);
                digest.update(message.getBody());
                return String.valueOf(digest.digest());
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
