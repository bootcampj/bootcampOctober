package com.activemq.test;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public class TestApp {
	
	public static void main(String[] args) throws Exception {
		thread(new TestAppProducer(), false);
		thread(new TestAppProducer(), false);
		thread(new TestAppProducer.TestAppConsumer(), false);
		Thread.sleep(1000);
		thread(new TestAppProducer.TestAppConsumer(), false);
		thread(new TestAppProducer(), false);
		thread(new TestAppProducer.TestAppConsumer(), false);
		thread(new TestAppProducer(), false);
		thread(new TestAppProducer.TestAppConsumer(), false);
		Thread.sleep(1000);
	}
	
	public static void thread(Runnable runnable, boolean daemon) {
		Thread brokerThread = new Thread(runnable);
		brokerThread.setDaemon(daemon);
		brokerThread.start();
	}
	
	public static class TestAppProducer implements Runnable{
		
		public void run(){
			try{
				//Create a Connection
				ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
				
				//create a connection
				Connection connection = connectionFactory.createConnection();
				connection.start();
				
				//create a session
				Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
				
				//create a destination(Topic or Queue)
				Destination destination = session.createQueue("TEST.FOO");
				
				//create a MessageProducer from the Session to the Topic or Queue
				MessageProducer producer = session.createProducer(destination);
				producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
				
				//Create a Message
				String text = "Hello World From: " + Thread.currentThread().getName() + " : " + this.hashCode();
				TextMessage message = session.createTextMessage(text);
				
				//Tell a producerto send a message
				System.out.println("Sent message: " + message.hashCode() + " : " + Thread.currentThread().getName());
				producer.send(message);
				
				//Clean Up
				session.close();
				connection.close();
			}catch(Exception e){
				System.out.println("Caught : " + e);
				e.printStackTrace();
			}
		}
		
		public static class TestAppConsumer implements Runnable, ExceptionListener{
			public void run(){
				
				try{
					
					//Create a Connection Factory
					ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
					
					//create a connecton
					Connection connection = connectionFactory.createConnection();
					connection.start();
					
					connection.setExceptionListener(this);
					
					//Create a Session 
					Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
					
					//Create a destinaton(Topic or Queue)
					Destination destination = session.createQueue("TEST.FOO");
					
					//Create a Message Consumer from the session to the topic or queue
					MessageConsumer consumer = session.createConsumer(destination);
					
					//Wait for a Message
					Message message = consumer.receive(1000);
					
					if (message instanceof TextMessage) {
						TextMessage textMessage = (TextMessage)message;
						String text = textMessage.getText();
						System.out.println("Received: " +  text);
					}else{
						System.out.println("Received: " + message);
					}
					
					consumer.close();
					session.close();
					connection.close();
					
				}catch(Exception e){
					System.out.println("Caught: " + e);
					e.printStackTrace();
				}
				
			}

			@Override
			public void onException(JMSException arg0) {
				System.out.println("JMS Exception occured.  Shutting down Client");
			}
		}
		
	}

}
