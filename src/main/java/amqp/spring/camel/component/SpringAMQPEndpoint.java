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

import java.util.StringTokenizer;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.TopicExchange;

/**
 * RabbitMQ Consumer URIs are in the format of:<br/>
 * <code>spring-amqp:exchange:queue:routingKey?params=values</code><br/>
 * 
 * RabbitMQ Producer URIs are in the format of:<br/>
 * <code>spring-amqp:exchange:routingKey?params=values</code><br/>
 * 
 * Producers can also defer the routing key to the message header, in which case the URI could be:<br/>
 * <code>spring-amqp:exchange?params=values</code><br/>
 * And the ROUTING_KEY header could be set to the appropriate routing key.
 */
public class SpringAMQPEndpoint extends DefaultEndpoint {
    private static transient final Logger LOG = LoggerFactory.getLogger(SpringAMQPEndpoint.class);
    
    protected AmqpAdmin amqpAdministration;
    protected AmqpTemplate amqpTemplate;
            
    String exchangeName;
    String queueName;
    String routingKey;
    String exchangeType = "direct";
    boolean durable = false;
    boolean exclusive = false;
    boolean autodelete = true;
    boolean transactional = false;
    int concurrentConsumers = 1;
    
    public SpringAMQPEndpoint(String remaining, AmqpTemplate template, AmqpAdmin admin) {
        LOG.info("Creating endpoint for {}", remaining);
        this.amqpAdministration = admin;
        this.amqpTemplate = template;
        
        String[] tokens = new String[3];
        StringTokenizer uriTokenizer = new StringTokenizer(remaining, ":");
        if(uriTokenizer.hasMoreTokens())
            tokens[0] = uriTokenizer.nextToken();
        if(uriTokenizer.hasMoreTokens())
            tokens[1] = uriTokenizer.nextToken();
        if(uriTokenizer.hasMoreTokens())
            tokens[2] = uriTokenizer.nextToken();
        
        this.exchangeName = tokens[0] == null ? "" : tokens[0]; //Per spec expected default is empty string
        if(tokens[2] == null) { //Producers need only specify exchange. Routing key optional.
            this.routingKey = tokens[1];
        } else { //Consumers must specify exchange, queue and routing key in that order
            this.queueName = tokens[1];
            this.routingKey = tokens[2];
        }
    }

    @Override
    public Producer createProducer() throws Exception {
        if(this.exchangeName == null)
            throw new IllegalStateException("Cannot have null exchange name");
        
        return new SpringAMQPProducer(this);
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        if(this.exchangeName == null)
            throw new IllegalStateException("Cannot have null exchange name");
        if(this.queueName == null)
            throw new IllegalStateException("Cannot have null queue name for exchange "+this.exchangeName);
        
        return new SpringAMQPConsumer(this, processor);
    }

    public AmqpAdmin getAmqpAdministration() {
        return amqpAdministration;
    }

    public void setAmqpAdministration(AmqpAdmin amqpAdministration) {
        this.amqpAdministration = amqpAdministration;
    }

    public AmqpTemplate getAmqpTemplate() {
        return amqpTemplate;
    }

    public void setAmqpTemplate(AmqpTemplate amqpTemplate) {
        this.amqpTemplate = amqpTemplate;
    }

    public int getConcurrentConsumers() {
        return concurrentConsumers;
    }

    public void setConcurrentConsumers(int concurrentConsumers) {
        this.concurrentConsumers = concurrentConsumers;
    }

    public boolean isTransactional() {
        return transactional;
    }

    public void setTransactional(boolean transactional) {
        this.transactional = transactional;
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public boolean isAutodelete() {
        return autodelete;
    }

    public void setAutodelete(boolean autodelete) {
        this.autodelete = autodelete;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public String getType() {
        return exchangeType;
    }

    public void setType(String exchangeType) {
        this.exchangeType = exchangeType;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public void setExclusive(boolean exclusive) {
        this.exclusive = exclusive;
    }

    @Override
    public boolean isSingleton() {
        //TODO Technically is it true that there can always be multiple consumers, even for AMQP queues?
        return false;
    }

    @Override
    protected String createEndpointUri() {
        return "spring-amqp:"+this.exchangeName+":"+this.routingKey;
    }
    
    org.springframework.amqp.core.Exchange createAMQPExchange() {
        if("direct".equals(this.exchangeType)) {
            return new DirectExchange(this.exchangeName, this.durable, this.autodelete);
        } else if("fanout".equals(this.exchangeType)) {
            return new FanoutExchange(this.exchangeName, this.durable, this.autodelete);
        } else if("headers".equals(this.exchangeType)) {
            return new HeadersExchange(this.exchangeName, this.durable, this.autodelete);
        } else if("topic".equals(this.exchangeType)) {
            return new TopicExchange(this.exchangeName, this.durable, this.autodelete);
        } else {
            return new DirectExchange(this.exchangeName, this.durable, this.autodelete);
        }
    }
}
