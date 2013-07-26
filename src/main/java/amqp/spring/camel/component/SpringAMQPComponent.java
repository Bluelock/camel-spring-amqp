/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */

package amqp.spring.camel.component;

import java.util.Map;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

public class SpringAMQPComponent extends DefaultComponent {
    private static transient final Logger LOG = LoggerFactory.getLogger(SpringAMQPComponent.class);
    
    protected ConnectionFactory connectionFactory;
    protected AmqpTemplate amqpTemplate;
    protected AmqpAdmin amqpAdministration;
    public static final String ROUTING_KEY_HEADER = "ROUTING_KEY";
    public static final String EXCHANGE_NAME_HEADER = "EXCHANGE_NAME";
    
    public SpringAMQPComponent() {
        this.connectionFactory = new CachingConnectionFactory();
    }
    
    public SpringAMQPComponent(CamelContext context) {
        super(context);
        
        //Attempt to load a connection factory from the registry
        if(this.connectionFactory == null) {
            Map<String, ConnectionFactory> factories = context.getRegistry().findByTypeWithName(ConnectionFactory.class);
            if(factories != null && ! factories.isEmpty()) {
                this.connectionFactory = factories.values().iterator().next();
                LOG.info("Found AMQP ConnectionFactory in registry for {}", this.connectionFactory.getHost());
            }
        }
        
        if(this.connectionFactory == null) {
            LOG.error("Cannot find a connection factory!");
        }
    }
    
    public SpringAMQPComponent(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        SpringAMQPEndpoint endpoint = new SpringAMQPEndpoint(this, uri, remaining, getAmqpTemplate(), getAmqpAdministration());
        setProperties(endpoint, parameters);
        return endpoint;
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public AmqpAdmin getAmqpAdministration() {
        if(this.amqpAdministration == null && getCamelContext() != null && getCamelContext().getRegistry() != null) {
            //Attempt to load an administration connection from the registry
            Map<String, AmqpAdmin> factories = getCamelContext().getRegistry().findByTypeWithName(AmqpAdmin.class);
            if(factories != null && ! factories.isEmpty()) {
                this.amqpAdministration = factories.values().iterator().next();
                LOG.info("Found AMQP Administrator in registry");
            }
        }
        
        if(this.amqpAdministration == null) {
            //Attempt to construct an AMQP Adminstration instance
            this.amqpAdministration = new RabbitAdmin(this.connectionFactory);
            LOG.info("Created new AMQP Administration instance");
        }
        
        return this.amqpAdministration;
    }

    public void setAmqpAdministration(AmqpAdmin amqpAdministration) {
        this.amqpAdministration = amqpAdministration;
    }

    public AmqpTemplate getAmqpTemplate() {
        if(this.amqpTemplate == null && getCamelContext() != null && getCamelContext().getRegistry() != null) {
            //Attempt to load an AMQP template from the registry
            Map<String, AmqpTemplate> factories = getCamelContext().getRegistry().findByTypeWithName(AmqpTemplate.class);
            if(factories != null && ! factories.isEmpty()) {
                this.amqpTemplate = factories.values().iterator().next();
                LOG.info("Found AMQP Template in registry");
            }
        }
        
        if(this.amqpTemplate == null) {
            //Attempt to construct an AMQP template
            this.amqpTemplate = new RabbitTemplate(this.connectionFactory);
            LOG.info("Created new AMQP Template");
        }
        
        return this.amqpTemplate;
    }

    public void setAmqpTemplate(AmqpTemplate amqpTemplate) {
        this.amqpTemplate = amqpTemplate;
    } 
    
    public static Throwable findRootCause(Throwable t) {
        if(t.getCause() == null) return t;
        return findRootCause(t.getCause());
    }
}

