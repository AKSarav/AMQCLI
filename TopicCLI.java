/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author aksarav
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import static java.lang.System.exit;
import java.net.URISyntaxException;
import java.util.logging.Level;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.Topic;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicCLI {

    public static final Logger logger = LoggerFactory.getLogger(TopicCLI.class);
    
    private Connection con;
    private Session ses;
    private MessageProducer producer;
    private MessageConsumer consumer;
    private MessageConsumer DurableConsumer;
    boolean quitNow;
    String ProducerName;
    String ConsumerName;
    String un="";
    String pw="";
    String url="";
    private static String TN="";
  private static BufferedReader msgstream = new BufferedReader(new InputStreamReader(System.in));
    
    
    public void initiate(String TopicName,String mode) throws JMSException, URISyntaxException, IOException, InterruptedException
    {
        
        if (un.isEmpty() || pw.isEmpty() || url.isEmpty())
            {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.print("\nEnter the details to Connect to ActiveMQ");
        System.out.print("\nEnter the UserName : ");
        un=br.readLine();
        
         java.io.Console console = System.console();
    if ( console instanceof java.io.Console)
    {
        System.out.print("Enter the Password : ");
        char[] pwd=console.readPassword();
        //Convert char array to String
        pw = new String(pwd);
        
    }
    else
    {
        System.out.print("Enter the Password : ");
        pw=br.readLine();
    }
    
        System.out.print("Enter the ConnectionURL : ");
        url=br.readLine();
                }
        else
        {
            logger.info("Taking Already Entered Username,Password and ActiveMQ URL");
        }
        if (un.equals(null) || pw.equals(null) || url.equals(null) || un.trim().length() == 0 || pw.trim().length() == 0 || url.trim().length() == 0 || un.equals("") || pw.equals("") || url.equals(""))
        {
          logger.error("Either Username or Password or BrokerURL is null/empty. Correct it");
          exit(2);
        }
        else
        {
        con = ActiveMQConnection.makeConnection(un,pw,url);
        //connection = ActiveMQConnection.makeConnection("tcp://localhost:61616");
        } 
        
        if ( mode.equalsIgnoreCase("publish"))
        {
            
            /*con = ActiveMQConnection.makeConnection("admin","c0mp!ex@01","tcp://localhost:61616");*/
            con.setClientID("Publish-TopicCLI");
            ses = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = ses.createTopic(TopicName);
            logger.info("Creating Topic ( if not already exists) :"+topic);
            logger.info("Creating MessageProducer");
            producer = ses.createProducer(topic);
         }
        else if ( mode.equalsIgnoreCase("subscribe"))
        {
            
            /*con = ActiveMQConnection.makeConnection("admin","c0mp!ex@01","tcp://localhost:61616");*/
            con.setClientID("MessageListener-TopicCLI");
            ses = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            
            Topic topic = ses.createTopic(TopicName);
            logger.info("Creating Topic ( if not already exists) :"+topic);
            logger.info("Creating Consumer");
            consumer = ses.createConsumer(topic);
            
            
            //Setting ConsumerName, This name can be used to identify the consumer at ActiveMQ
            ConsumerName="TOPICCLI_Consumer";
            consumer.setMessageListener(new ConsumerML(ConsumerName));
            logger.info("Message Listner is configured with ConsumeName:"+ConsumerName);
            con.start();
            
            
        }
        else if ( mode.equalsIgnoreCase("durable"))
        {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            /*con = ActiveMQConnection.makeConnection("admin","c0mp!ex@01","tcp://localhost:61616");*/
            //con.setClientID("DurableSubscribe-TopicCLI");
            System.out.println("\nNote*: Enter the Already available (or) new Subcriber and ConsumerName Combination. In order to receive Pending Message use the Already created Subscriber/consumer names");
            System.out.print("\nEnter SubscriberName => ");
            String SubscriberName = reader.readLine();
            System.out.print("Enter ConsumerName/ClientID => ");    
            String ConsumerName = reader.readLine();
            con.setClientID(ConsumerName);
            
            
            ses = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            con.start();
            
            Topic topic = ses.createTopic(TopicName);
            logger.info("Creating Topic ( if not already exists) :"+topic);
            logger.info("Creating Subscriber with Name "+SubscriberName);
            logger.info("Creating Consumer with Name "+ConsumerName);
            //createDurableSubscriber(topic,consumername,messageselector,nolocal boolen)
            consumer = ses.createDurableSubscriber(topic,SubscriberName,"", false);
            
            
            
            
            logger.info("Consumer is going receive all Pending/Enqueued Messages from the Topic in a Loop");
            while (true)        
            {
                TextMessage outboundmsg;
                outboundmsg = (TextMessage) consumer.receive(1000);
                if (outboundmsg instanceof TextMessage)
                {
                logger.info( ConsumerName+" received a New Message with Text { " + outboundmsg.getText() + " }");
                }
                else
                {
                logger.info("Topic is Empty,Closing the connection");
                logger.info("Consumer will wait for 1000 milliseconds before Returning");
                break;
                
                
                }
            }
            
        }
        
        
    }
    
   
    
    
    public void sendit(String Message) throws JMSException
    {
        TextMessage txtmesg = ses.createTextMessage(Message);
        //producer.send(txtmesg);    
        producer.send(txtmesg, javax.jms.DeliveryMode.PERSISTENT, javax.jms.Message.DEFAULT_PRIORITY, javax.jms.Message.DEFAULT_TIME_TO_LIVE);
        System.out.println();
        
        logger.info("JMS Message Sent: { "+txtmesg.getText()+" }");
        
    }
    
    public void TopicSender() throws IOException, JMSException, URISyntaxException
    {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try
        {
            quitNow = false;
            
            do
            {
                System.out.print("\nEnter the TopicName (or) \"quit\"/\"Quit\" to exit ["+ TN +"] :");          
                String TNI = msgstream.readLine();
                TNI = TNI.trim();

                if ( TNI == "" || TNI.equals(null) || TNI.trim().length() == 0 && TN.isEmpty())
                {
                  logger.error("Queue name is Invalid, Please Enter Valid QueueName");
                }
                else if ( TNI == "" || TNI.equals(null) || TNI.trim().length() == 0 )
                {
                  logger.info("Taking the Already Entered QueueName - "+TN);
                }
                else
                {
                    TN=TNI;
                } 
                
                if (TN.equalsIgnoreCase("quit"))
                {
                    String ans;
                    int choice;
                    System.out.print("You have entered Quit, We are not sure if that's Topic Name.");
                    System.out.print ("\n\nPlease Clarify? \n1 => Press One to Exit \n2 => Press 2 to Reset & Continue \n3 => Quit is my Topic Name\n\nYour Option :");
                    ans=br.readLine();
                    choice=Integer.parseInt(ans);
                    switch (choice)
                    {
                        case 1: quitNow=true; 
                                this.wrapitup();
                                System.exit(0);
                                break;
                        case 2: quitNow=false;
                                break;
                        case 3: quitNow=false;
                                this.initiate(TN,"publish");
                                System.out.println("\nEnter the Message to be sent");
                                String MSG;
                                MSG = br.readLine();
                                this.sendit(MSG);
                                logger.info("Back to the Future");
                                this.wrapitup();
                                break;
                        default:
                                logger.error("No Such Option Sorry! Try Again");
                                quitNow=false;
                                break;

                    }
                }
                else
                {
                    quitNow=false;
                    this.initiate(TN,"publish");
                    System.out.println("\nEnter the Message to be sent");
                    String MSG;
                    MSG = br.readLine();
                    this.sendit(MSG);
                    logger.info("Back to the Future");
                    this.wrapitup();
                    
                }
            } while(! quitNow);
       
            
        }
        catch (Exception ex) {
          java.util.logging.Logger.getLogger(QueueSender.class.getName()).log(Level.SEVERE, null, ex);
      }
  
        
    }
    
    
     public void TopicReceiver(String mode) throws IOException, JMSException, URISyntaxException
    {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try
        {
            quitNow = false;
            
            do
            {
                
                System.out.print("\nEnter the TopicName (or) \"quit\"/\"Quit\" to exit ["+ TN +"] :");      
                String TNI = msgstream.readLine();
                TNI = TNI.trim();

                if ( TNI == "" || TNI.equals(null) || TNI.trim().length() == 0 && TN.isEmpty())
                {
                  logger.error("Queue name is Invalid, Please Enter Valid QueueName");
                }
                else if ( TNI == "" || TNI.equals(null) || TNI.trim().length() == 0 )
                {
                  logger.info("Taking the Already Entered QueueName - "+TN);
                }
                else
                {
                    TN=TNI;
                }
                
                if (TN.equalsIgnoreCase("quit"))
                {
                    String ans;
                    int choice;
                    System.out.print("You have entered Quit, We are not sure if that's Topic Name.");
                    System.out.print ("\n\nPlease Clarify? \n1 => Press One to Exit \n2 => Press 2 to Reset & Continue \n3 => Quit is my Topic Name\n\nYour Option :");
                    ans=br.readLine();
                    choice=Integer.parseInt(ans);
                    switch (choice)
                    {
                        case 1: quitNow=true; 
                                this.wrapitup();
                                System.out.println("GoodBye!");
                                System.exit(0);
                                break;
                        case 2: quitNow=false;
                                break;
                        case 3: quitNow=false;
                                this.initiate(TN,"subscribe");
                                Thread.sleep(20000);
                                break;
                        default:
                                logger.error("No Such Option Sorry! Try Again");
                                quitNow=false;
                                break;

                    }
                }
                else
                {
                    quitNow=false;
                    this.initiate(TN,mode);
                    Thread.sleep(300000);
                    
                    
                    this.wrapitup();
                    break;
                }
            } while(! quitNow);
       
            
        }
        catch (Exception ex) {
          java.util.logging.Logger.getLogger(QueueSender.class.getName()).log(Level.SEVERE, null, ex);
      }
  
        
    }
    
    
    
    
    public void wrapitup() throws JMSException
    {
        con.close();
        ses.close();
    }
    
    public static void main(String args[]) throws IOException, JMSException, URISyntaxException
    {
        
      String Option = null;
        
      System.out.println("-------------------------------");
      System.out.println("ACTIVEMQ - TOPIC CLI");
      System.out.println("-------------------------------");
      
      try
      {
      
      Option=args[0];
      
      if (Option.equals(null) || Option.trim().length() == 0 || Option.equals("") )
      {
          logger.error("Invalid (or) Null Argument, No Such Task available");
          System.out.println("");
          logger.info("java -cp AMQ-TOPIC-CLI.jar [publish|subscribe|durable]");
          logger.info(" publish   - To publish messages to the Topic");
          logger.info(" subscribe - To subscribe to the Topic");
          logger.info(" durable   - To durable subscribe to the Topic");
          exit(2);
      }
      
      if (Option.equalsIgnoreCase("publish") ||  Option.equalsIgnoreCase("subscribe") ||  Option.equalsIgnoreCase("durable"))
      {
          logger.info("Task Validation Successful");
      }
      else
      {
          logger.error("Task is Invalid, No Such Task available : "+Option);
          logger.error("Supported Tasks are publish, subscribe, durable ");
      }
      }
      catch (java.lang.ArrayIndexOutOfBoundsException ex)
              {
                  logger.error("ERROR: IndexOutofBoundException");
                  logger.error("Argument is mendatory");
                    logger.info("java -cp AMQ-TOPIC-CLI.jar [publish|subscribe|durable]");
                    logger.info("   publish   - To publish messages to the Topic");
                    logger.info("   subscribe - To subscribe to the Topic");
                    logger.info("   durable   - To durable subscribe to the Topic");
                  exit(2);
              }
      
        
        if ( Option.equalsIgnoreCase("publish"))
        {
            logger.info("Invoking TopicCLI for Publish/Sending Task ", Option);
            TopicCLI tcli = new TopicCLI();
            tcli.TopicSender();
        }
        else if (Option.equalsIgnoreCase("subscribe"))
        {
            logger.info("Invoking TopicCLI for Receiving/Subscribe Task ", Option);
            TopicCLI tcli = new TopicCLI();
            tcli.TopicReceiver("subscribe");
        }
        else if (Option.equalsIgnoreCase("durable"))
        {
            logger.info("Invoking TopicCLI for Receiving/Subscribe Task ", Option);
            TopicCLI tcli = new TopicCLI();
            tcli.TopicReceiver("durable");
        }
    
    }
}
