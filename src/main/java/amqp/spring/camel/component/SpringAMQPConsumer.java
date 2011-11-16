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

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.camel.impl.DefaultExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
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
        
        if(this.binding != null) {
            this.endpoint.getAmqpAdministration().declareBinding(binding);
            LOG.info("Declared binding {}", this.binding.getRoutingKey());
        }
        
        this.messageListener.start();
    }

    @Override
    public void stop() throws Exception {
        this.messageListener.stop();
        
        super.stop();
    }

    @Override
    public void shutdown() throws Exception {
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
        public void onMessage(Message amqpMessage) {
            LOG.debug("Received message for routing key {}", amqpMessage.getMessageProperties().getReceivedRoutingKey());

            SpringAMQPMessage camelMessage = SpringAMQPMessage.fromAMQPMessage(msgConverter, amqpMessage);
            Exchange exchange = new DefaultExchange(endpoint, endpoint.getExchangePattern());
            exchange.setIn(camelMessage);

            try {
                getProcessor().process(exchange);
            } catch(Throwable t) {
                exchange.setException(t);
            }
            
            //Send a reply if one was requested
            Address replyToAddress = amqpMessage.getMessageProperties().getReplyToAddress();
            if(replyToAddress != null) {
                SpringAMQPMessage outMessage = exchange.getOut(SpringAMQPMessage.class);
                endpoint.getAmqpTemplate().send(replyToAddress.getExchangeName(), replyToAddress.getRoutingKey(), outMessage.toAMQPMessage(msgConverter));
            }
        }
    }
}
