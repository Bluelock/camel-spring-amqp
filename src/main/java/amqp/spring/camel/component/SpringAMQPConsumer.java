/**
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License version 2.0 (the "License"). You can obtain a copy of the
 * License at http://mozilla.org/MPL/2.0/.
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.camel.impl.DefaultExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.converter.MessageConverter;

public class SpringAMQPConsumer extends DefaultConsumer {
    private static transient final Logger LOG = LoggerFactory.getLogger(SpringAMQPConsumer.class);
    
    protected SpringAMQPEndpoint endpoint;
    private RabbitMQConsumerTask messageListener;
    private Queue queue;
    private Binding binding;
    
    public SpringAMQPConsumer(SpringAMQPEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
        this.messageListener = new RabbitMQConsumerTask((RabbitTemplate) this.endpoint.getAmqpTemplate(), this.endpoint.queueName, this.endpoint.getConcurrentConsumers());
    }

    @Override
    public void start() throws Exception {
        super.start();
        
        org.springframework.amqp.core.Exchange exchange = this.endpoint.createAMQPExchange();
        this.endpoint.amqpAdministration.declareExchange(exchange);
        LOG.info("Declared exchange {}", exchange.getName());

        this.queue = new Queue(this.endpoint.queueName, this.endpoint.durable, this.endpoint.exclusive, this.endpoint.autodelete);
        this.endpoint.getAmqpAdministration().declareQueue(queue);
        LOG.info("Declared queue {}", this.queue.getName());
        
        if(exchange instanceof HeadersExchange) { //Is this a header exchange? Bind the key/value pair(s)
            Map<String, Object> keyValues = parseKeyValues(this.endpoint.routingKey);
            this.binding = BindingBuilder.bind(this.queue).to((HeadersExchange) exchange).whereAll(keyValues).match();
        } else {
            this.binding = BindingBuilder.bind(this.queue).to(exchange).with(this.endpoint.routingKey).noargs();
        }
        this.endpoint.getAmqpAdministration().declareBinding(binding);
        LOG.info("Declared binding {}", this.binding.getRoutingKey());
        
        this.messageListener.start();
    }

    @Override
    public void stop() throws Exception {
        this.messageListener.stop();
        
        if(this.endpoint.amqpAdministration != null && this.binding != null) {
            this.endpoint.amqpAdministration.removeBinding(this.binding);
            LOG.info("Removed binding {}", this.binding.getRoutingKey());
        }
        
        if(this.endpoint.amqpAdministration != null && this.queue != null) {
            this.endpoint.amqpAdministration.deleteQueue(this.queue.getName());
            LOG.info("Deleted queue {}", this.queue.getName());
        }
        
        super.stop();
    }

    @Override
    public void shutdown() throws Exception {
        this.messageListener.shutdown();
        super.shutdown();
    }
    
    protected static Map<String, Object> parseKeyValues(String routingKey) {
        if(routingKey.contains("|"))
            throw new IllegalArgumentException("Sorry, OR boolean not yet supported, only AND.");
        
        StringTokenizer tokenizer = new StringTokenizer(routingKey, "&|");
        Map<String, Object> pairs = new HashMap<String, Object>();
        while(tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            String[] keyValue = token.split("=");
            pairs.put(keyValue[0], keyValue[1]);
        }
        
        return pairs;
    }

    //We have to ask the RabbitMQ Template for converters, the interface doesn't have a way to get MessageConverter
    class RabbitMQConsumerTask implements MessageListener {
        private MessageConverter msgConverter;
        private SimpleMessageListenerContainer listenerContainer;
        
        public RabbitMQConsumerTask(RabbitTemplate template, String queue, int concurrentConsumers) {
            this.msgConverter = template.getMessageConverter();
            this.listenerContainer = new SimpleMessageListenerContainer();
            this.listenerContainer.setConnectionFactory(template.getConnectionFactory());
            this.listenerContainer.setQueueNames(queue);
            this.listenerContainer.setConcurrentConsumers(concurrentConsumers);
        }
        
        public void start() {
            this.listenerContainer.setMessageListener(this);
            this.listenerContainer.start();
        }
        
        public void stop() {
            this.listenerContainer.stop();
        }
        
        public void shutdown() {
            this.listenerContainer.shutdown();
        }

        @Override
        public void onMessage(Message message) {
            try {
                LOG.trace("Received message for routing key {}", message.getMessageProperties().getReceivedRoutingKey());

                Object body = msgConverter.fromMessage(message);

                Exchange exchange = new DefaultExchange(endpoint, endpoint.getExchangePattern());
                exchange.getIn().setBody(body);
                for(Entry<String, Object> headerEntry : message.getMessageProperties().getHeaders().entrySet())
                    exchange.getIn().setHeader(headerEntry.getKey(), headerEntry.getValue());

                getProcessor().process(exchange);
            } catch (IOException e) {
                LOG.error("Error when attempting to speak with RabbitMQ", e);
            } catch (InterruptedException e) {
                LOG.warn("Thread was interrupted while waiting for message consumption", e);
            } catch (Exception e) {
                LOG.warn("General exception during Camel handoff, Processor returned error", e);
            }
        }
    }
}
