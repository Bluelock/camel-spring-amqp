/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */

package amqp.spring.camel.component;

import com.rabbitmq.client.Channel;
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
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.retry.MessageKeyGenerator;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.ErrorHandler;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class SpringAMQPConsumer extends DefaultConsumer implements ConnectionListener {
    private static transient final Logger LOG = LoggerFactory.getLogger(SpringAMQPConsumer.class);
    private static final String TTL_QUEUE_ARGUMENT = "x-message-ttl";
    private static final String HA_POLICY_ARGUMENT = "x-ha-policy";

    protected SpringAMQPEndpoint endpoint;
    private RabbitMQMessageListener messageListener;

    public SpringAMQPConsumer(SpringAMQPEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
        this.messageListener = new RabbitMQMessageListener(endpoint);
    }

    @Override
    public void doStart() throws Exception {
        super.doStart();

        if(! this.messageListener.listenerContainer.isActive())
            this.messageListener.start();
    }

    @Override
    public void doShutdown() throws Exception {
        this.messageListener.shutdown();
        super.shutdown();
    }

    @Override
    public void doStop() throws Exception {
        this.messageListener.shutdown();
        super.doStop();
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
        LOG.info("Network connection created to broker for endpoint {}", this.getEndpoint());
    }

    @Override
    public void onClose(Connection connection) {
        // This event is received when the consumer initiates a close,
        // but this event is _not_ received when RabbitMQ is the one that breaks the connection.
        LOG.info("Network connection closed to broker for endpoint {}", this.getEndpoint());
    }
    
    //We have to ask the RabbitMQ Template for converters, the interface doesn't have a way to get MessageConverter
    class RabbitMQMessageListener implements ChannelAwareMessageListener {
        private MessageConverter msgConverter;
        private SimpleMessageListenerContainer listenerContainer;
        private static final long DEFAULT_TIMEOUT_MILLIS = 1000;

        public RabbitMQMessageListener(SpringAMQPEndpoint endpoint) {
            this.listenerContainer = new SimpleMessageListenerContainer();
            this.listenerContainer.setTaskExecutor(new SpringAMQPExecutor(endpoint));

            RabbitTemplate template = (RabbitTemplate) endpoint.getAmqpTemplate();
            if(template != null) {
                this.msgConverter = template.getMessageConverter();
                this.listenerContainer.setConnectionFactory(template.getConnectionFactory());
            } else {
                LOG.error("No AMQP Template found! Cannot initialize message conversion or connections!");
            }

            this.listenerContainer.setQueueNames(endpoint.getQueueName());
            this.listenerContainer.setConcurrentConsumers(endpoint.getConcurrentConsumers());
            this.listenerContainer.setPrefetchCount(endpoint.getPrefetchCount());
            this.listenerContainer.setAcknowledgeMode(endpoint.getAcknowledgeMode());

            //Set error handling (send it to Camel)
            this.listenerContainer.setErrorHandler(getErrorHandler());
            this.listenerContainer.setAdviceChain(getAdviceChain());

            //Set timeouts
            this.listenerContainer.setShutdownTimeout(DEFAULT_TIMEOUT_MILLIS);
            this.listenerContainer.setReceiveTimeout(DEFAULT_TIMEOUT_MILLIS);
            this.listenerContainer.setRecoveryInterval(DEFAULT_TIMEOUT_MILLIS / 2);

            //Transactions are currently not supported
            this.listenerContainer.setChannelTransacted(false);
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
            retryOperation.setMessageKeyGenerator(new DefaultKeyGenerator());
            
            return new Advice[] { retryOperation.getObject() };
        }

        @Override
        public void onMessage(Message amqpMessage, Channel channel) {
            if(this.msgConverter == null)
                throw new IllegalStateException("No message converter present - cannot processs messages!");
            
            LOG.debug("Received message for routing key {}", amqpMessage.getMessageProperties().getReceivedRoutingKey());
            ExchangePattern exchangePattern = SpringAMQPMessage.getExchangePattern(amqpMessage);
            Exchange exchange = new DefaultExchange(endpoint, exchangePattern);
            SpringAMQPMessage camelMessage = SpringAMQPMessage.fromAMQPMessage(msgConverter, amqpMessage);
            exchange.setIn(camelMessage);
            
            try {
                getProcessor().process(exchange);

                if (endpoint.getAcknowledgeMode() == AcknowledgeMode.MANUAL) {
                    long deliveryTag = amqpMessage.getMessageProperties().getDeliveryTag();
                    LOG.trace("Acknowledging receipt [delivery_tag={}]", deliveryTag);
                    channel.basicAck(deliveryTag, false);
                }
            } catch(Throwable t) {
                exchange.setException(t);
            }
            
            //Send a reply if one was requested
            Address replyToAddress = amqpMessage.getMessageProperties().getReplyToAddress();
            if(replyToAddress != null && endpoint.isAutoReply()) {
                org.apache.camel.Message outMessage = exchange.getOut();
                SpringAMQPMessage replyMessage = new SpringAMQPMessage(outMessage);

                // Camel exchange will contain a non-null exception if an unhandled exception has occurred,
                // such as when using the DefaultErrorHandler with default configuration, or when
                // using the DeadLetterChannel error handler with an OnException handled=false override.
                // Exchange will not contain an exception (via getException()) if the exception has been handled,
                // such as when using the DeadLetterChannel error handler with default configuration, but
                // the Exchange property EXCEPTION_CAUGHT will contain the handled exception.
                if (exchange.getException() != null) {
                    replyMessage.setHeader(SpringAMQPMessage.IS_EXCEPTION_CAUGHT, true);
                    replyMessage.setBody(exchange.getException());
                } else if (exchange.getProperty(Exchange.EXCEPTION_CAUGHT) != null) {
                    replyMessage.setHeader(SpringAMQPMessage.IS_EXCEPTION_CAUGHT, true);
                    replyMessage.setBody(exchange.getProperty(Exchange.EXCEPTION_CAUGHT));
                }

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

    /**
     * Declaration of AMQP exchange, queue, and binding as well as re-try logic
     * all injected into SimpleMessageListenerContainer via its taskExecutor,
     * due to the following three problems:
     * 1.) Broker restarts with non-durable queues (where the new broker instance will not
     * contain the non-durable queues that existed in the old broker instance)
     * will create a failure of the BlockingQueueConsumer due to its unfortunate reliance
     * on passive declaration (which fails if the queue does not exist).
     * 2.) Load balancer failover of one broker to another will maintain the client's network
     * connection so we cannot rely on network connection events.
     * 3.) Both SimpleMessageListenerContainer and BlockingQueueConsumer have important
     * fields that are private without any getters, so subclassing either class fails to
     * provide access to needed functionality.
     */
    class SpringAMQPExecutor extends SimpleAsyncTaskExecutor {
        private SpringAMQPEndpoint endpoint;

        SpringAMQPExecutor(SpringAMQPEndpoint endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public void execute(final Runnable task) {
            Runnable enrichedTask = new SpringAMQPExecutorTask(endpoint, task);
            super.execute(enrichedTask);
        }

        @Override
        public void execute(final Runnable task, long startTimeout) {
            Runnable enrichedTask = new SpringAMQPExecutorTask(endpoint, task);
            super.execute(enrichedTask, startTimeout);
        }

    }

    class SpringAMQPExecutorTask implements Runnable {
        private SpringAMQPEndpoint endpoint;
        private Runnable delegateTask;

        // Retry every 30 seconds upon error
        public static final long RECOVERY_INTERVAL_MILLISECONDS = 30000L;


        public SpringAMQPExecutorTask(SpringAMQPEndpoint endpoint, Runnable delegateTask) {
            this.endpoint = endpoint;
            this.delegateTask = delegateTask;
        }

        @Override
        public void run() {
            boolean error;

            do {
                try {
                    error = false;
                    declareAMQPEntities();
                    delegateTask.run();
                } catch (Exception e) {
                    error = true;
                    LOG.error("Error consuming endpoint " + endpoint + ". " + e.getMessage(), e);
                    try {
                        Thread.sleep(RECOVERY_INTERVAL_MILLISECONDS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Unrecoverable interruption on consumer restart");
                    }
                }
            } while (error && !endpoint.isStoppingOrStopped());
        }

        protected void declareAMQPEntities() {
            org.springframework.amqp.core.Exchange exchange = declareExchange();
            Queue queue = declareQueue();
            declareBinding(exchange, queue);
        }

        protected org.springframework.amqp.core.Exchange declareExchange() {
            org.springframework.amqp.core.Exchange exchange = this.endpoint.createAMQPExchange();
            if (this.endpoint.isUsingDefaultExchange()) {
                LOG.info("Using default exchange; will not declare one for endpoint {}.", endpoint);
            } else {
                try {
                    this.endpoint.amqpAdministration.declareExchange(exchange);
                    LOG.info("Declared exchange {} for endpoint {}.", exchange.getName(), endpoint);
                } catch (AmqpIOException e) {
                    LOG.warn(String.format("Could not declare exchange %s for endpoint %s; possible re-declaration of a different type?", exchange.getName(), endpoint.toString()), e);
                    // Be lenient:  Do not re-throw Exception because the exchange may already exist but just declared
                    // with different attributes, so let's go ahead and declare the queue and binding anyway.
                } catch (AmqpConnectException e) {
                    LOG.error(String.format("Consumer cannot connect to broker for endpoint %s", this.endpoint.toString()), e);
                    throw e;
                }
            }
            return exchange;
        }

        protected Queue declareQueue() {
            //Determine queue arguments, including vendor extensions
            Map<String, Object> queueArguments = new HashMap<String, Object>();
            if(endpoint.getTimeToLive() != null)
                queueArguments.put(TTL_QUEUE_ARGUMENT, endpoint.getTimeToLive());
            if(endpoint.isHa() )
                queueArguments.put(HA_POLICY_ARGUMENT, "all");

            //Declare queue
            Queue queue = new Queue(this.endpoint.queueName, this.endpoint.durable, this.endpoint.exclusive, this.endpoint.autodelete, queueArguments);
            this.endpoint.getAmqpAdministration().declareQueue(queue);
            LOG.info("Declared queue {} for endpoint {}.", queue.getName(), endpoint);
            return queue;
        }

        protected Binding declareBinding(org.springframework.amqp.core.Exchange exchange, Queue queue) {
            Binding binding = null;

            //Is this a header exchange? Bind the key/value pair(s)
            if(exchange instanceof HeadersExchange) {
                if(this.endpoint.routingKey == null)
                    throw new IllegalStateException("Specified a header exchange without a key/value match");

                if(this.endpoint.routingKey.contains("|") && this.endpoint.routingKey.contains("&"))
                    throw new IllegalArgumentException("You cannot mix AND and OR expressions within a header binding");

                Map<String, Object> keyValues = parseKeyValues(this.endpoint.routingKey);
                BindingBuilder.HeadersExchangeMapConfigurer mapConfig = BindingBuilder.bind(queue).to((HeadersExchange) exchange);
                if(this.endpoint.routingKey.contains("|"))
                    binding = mapConfig.whereAny(keyValues).match();
                else
                    binding = mapConfig.whereAll(keyValues).match();

            //Is this a fanout exchange? Just bind the queue and exchange directly
            } else if(exchange instanceof FanoutExchange) {
                binding = BindingBuilder.bind(queue).to((FanoutExchange) exchange);

            //Perform routing key binding for direct or topic exchanges
            } else {
                binding = BindingBuilder.bind(queue).to(exchange).with(this.endpoint.routingKey).noargs();
            }

            if (this.endpoint.isUsingDefaultExchange()) {
                LOG.info("Using default exchange for endpoint {}.  Default exchange is implicitly bound to every queue, with a routing key equal to the queue name.", endpoint);
            } else if (binding != null) {
                LOG.info("Declaring binding {} for endpoint {}.", binding.getRoutingKey(), endpoint);
                this.endpoint.getAmqpAdministration().declareBinding(binding);
            }

            return binding;
        }
    }
}
