package com.lucazamador.drools.camel;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.NamingException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.util.jndi.JndiContext;
import org.drools.KnowledgeBase;
import org.drools.builder.KnowledgeBuilder;
import org.drools.builder.KnowledgeBuilderError;
import org.drools.builder.KnowledgeBuilderFactory;
import org.drools.builder.ResourceType;
import org.drools.grid.GridNode;
import org.drools.grid.impl.GridImpl;
import org.drools.grid.service.directory.WhitePages;
import org.drools.grid.service.directory.impl.WhitePagesImpl;
import org.drools.io.impl.ClassPathResource;
import org.drools.runtime.StatefulKnowledgeSession;
import org.junit.Test;

/**
 * 
 * @author Lucas Amador
 * 
 */
public class CamelIntegrationTest {

    private StatefulKnowledgeSession ksession;
    private Session session;

    @Test
    public void simpleTest() throws Exception {

        BrokerService brokerService = new BrokerService();
        brokerService.addConnector("tcp://localhost:61616");
        brokerService.setBrokerName("embedded-broker");
        brokerService.setUseJmx(false);
        brokerService.start();

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection connection = connectionFactory.createConnection();
        connection.start();

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Thread t = new Thread(createRunnableMessageConsumer());
        t.start();

        CamelContext camelContext = createCamelContext();
        camelContext.start();

        Login login = new Login();
        login.setUsername("lucaz");
        login.setPassword("emiliano");

        ksession.insert(login);
        ksession.fireAllRules();

        brokerService.stop();
        camelContext.stop();
    }

    private CamelContext createCamelContext() throws Exception, NamingException {
        KnowledgeBuilder kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();
        kbuilder.add(new ClassPathResource("rules.drl", getClass()), ResourceType.DRL);

        if (kbuilder.hasErrors()) {
            if (kbuilder.getErrors().size() > 0) {
                for (KnowledgeBuilderError kerror : kbuilder.getErrors()) {
                    System.err.println(kerror);
                }
                throw new RuntimeException("rules with errors");
            }
        }
        KnowledgeBase kbase = kbuilder.newKnowledgeBase();
        ksession = kbase.newStatefulKnowledgeSession();

        GridImpl grid = new GridImpl();
        grid.addService(WhitePages.class, new WhitePagesImpl());
        GridNode node = grid.createGridNode("node");
        Context context = new JndiContext();
        context.bind("node", node);
        node.set("ksession", ksession);
        CamelContext camelContext = new DefaultCamelContext(context);
        RouteBuilder rb = new RouteBuilder() {
            public void configure() throws Exception {
                from("drools://node/ksession?channel=logins").to("activemq:queue:security-queue");
            }
        };
        camelContext.addRoutes(rb);
        return camelContext;
    }

    private Runnable createRunnableMessageConsumer() {
        return new Runnable() {
            public void run() {
                try {
                    Queue queue = session.createQueue("security-queue");
                    MessageConsumer consumer = session.createConsumer(queue);
                    Message message;
                    while ((message = consumer.receive()) != null) {
                        if (message instanceof ActiveMQTextMessage) {
                            ActiveMQTextMessage textMessage = (ActiveMQTextMessage) message;
                            System.out.println("[security-queue] Received message: " + textMessage.getText());
                        }
                    }
                } catch (JMSException e) {
                    // evil catch. don't do it at home.
                }
            }
        };
    }

}
