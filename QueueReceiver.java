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
import java.util.Arrays;
import java.util.Enumeration;
import java.util.logging.Level;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QueueReceiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueueReceiver.class);
  
  private String clientId;
  private Connection connection;
  private MessageConsumer messageConsumer;
  public String ReceivedMessage;
  String un="";
  String pw="";
  String url="";
  private static String QN=""; 
  private static BufferedReader msgstream = new BufferedReader(new InputStreamReader(System.in));
  private static String readmode;
  
  public void init(String queueName)
      throws JMSException, URISyntaxException, IOException {

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
        LOGGER.info("Taking Already Entered Username,Password and ActiveMQ URL");
    }
    if (un.equals(null) || pw.equals(null) || url.equals(null) || un.trim().length() == 0 || pw.trim().length() == 0 || url.trim().length() == 0 || un.equals("") || pw.equals("") || url.equals(""))
    {
      LOGGER.error("Either Username or Password or BrokerURL is null/empty. Correct it");
      exit(2);
    }
    else
    {
    connection = ActiveMQConnection.makeConnection(un,pw,url);
    //connection = ActiveMQConnection.makeConnection("tcp://localhost:61616");
    } 
  

  // create a Session
  Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

  // create the Queue from which messages will be received
  Queue queue = session.createQueue(queueName);
  
  
  /*QueueBrowser qb = session.createBrowser(queue);
  Enumeration qe = qb.getEnumeration();
  System.out.println(qe.hasMoreElements()); */
 
  
  
  
  // create a MessageConsumer for receiving messages
  messageConsumer = session.createConsumer(queue);
  

  // start the connection in order to receive messages
  connection.start();

  }

  public void closeConnection() throws JMSException {
    connection.close();
  }

  public String receivemsg(int timeout, boolean acknowledge)
      throws JMSException, InterruptedException {
      
     
      
      
    if (readmode.equalsIgnoreCase("allatonce"))
    {
    
    
    //Sleep for 3000 milliseconds, before processing the Queue. Let the consumer get ready
    Thread.sleep(3000);
      
    while(true)
    { 
    Message message = messageConsumer.receive(timeout);    
    // check if a message was received
    if (message instanceof Message)
    {
     TextMessage textMessage = (TextMessage) message;
     
      // retrieve the message content
      String text = textMessage.getText();
      LOGGER.info("received message with text='{}'",
          text);

      if (acknowledge) {
        // acknowledge the successful processing of the message
        message.acknowledge();
        LOGGER.info("message acknowledged");
      } else {
        LOGGER.info("message not acknowledged");
      }
      
      ReceivedMessage = text;
      LOGGER.info(ReceivedMessage);
    } else {
      LOGGER.info("no more messages to read from the queue");
      break;
    }
   
    }
    }
    else if (readmode.equalsIgnoreCase("onebyone"))
    {
        Message message = messageConsumer.receive(timeout);
    // check if a message was received
    
    if (message instanceof Message)
    {
     TextMessage textMessage = (TextMessage) message;
     
      // retrieve the message content
      String text = textMessage.getText();
      LOGGER.info("\nreceived message with text='{}'",
          text);

      if (acknowledge) {
        // acknowledge the successful processing of the message
        message.acknowledge();
        LOGGER.info("message acknowledged");
      } else {
        LOGGER.info("message not acknowledged");
      }
      
      ReceivedMessage = text;
      LOGGER.info(ReceivedMessage);
    } else {
      LOGGER.info("no more message to read from the queue");
      return ReceivedMessage;
    }
    
    }
      
    
    return null;
  }
  
  public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException
  {

      System.out.println("-------------------------------");
      System.out.println("ACTIVEMQ - QUEUE RECEIVER- CLI");
      System.out.println("-------------------------------");
      
      try
      {
          
     
      
      
      readmode = args[0];
      
      if (readmode.equals(null) || readmode.trim().length() == 0 || readmode.equals("") )
      {
          LOGGER.error("Argument is mendatory");
          LOGGER.info("java -cp AMQ-QUEUE-CLI.jar QueueReceiver [onebyone|allatonce]");
          exit(2);
      }
      
      if (readmode.equalsIgnoreCase("onebyone") &&  readmode.equalsIgnoreCase("allatonce"))
      {
          LOGGER.error("Allowed Arguements are onebyone (or) allatonce ");
      }
      }
      catch (java.lang.ArrayIndexOutOfBoundsException ex)
              {
                  LOGGER.error("ERROR: IndexOutofBoundException");
                  LOGGER.error("Argument is mendatory");
                  LOGGER.info("java -cp AMQ-QUEUE-CLI.jar QueueReceiver [onebyone|allatonce]");
                  exit(2);
              }
      
      QueueReceiver sobj = new QueueReceiver();
      try {

          
          
          boolean quitNow = false;
          do {
          
         
          System.out.print("\nEnter the Queue Name (or) quit/Quit to Exit ["+ QN +"] :");
          String QNI = msgstream.readLine();
          QNI = QNI.trim();
          
          if ( QNI == "" || QNI.equals(null) || QNI.trim().length() == 0 && QN.isEmpty())
          {
            LOGGER.error("Queue name is Invalid, Please Enter Valid QueueName");
          }
          else if ( QNI == "" || QNI.equals(null) || QNI.trim().length() == 0 )
          {
            LOGGER.info("Taking the Already Entered QueueName - "+QN);
          }
          else
          {
              QN=QNI;
          }
          
          
          
 
          
          
          if( QN.equalsIgnoreCase("Quit"))
          {
              String ans;
              int choice;
              System.out.print("You have entered Quit, We are not sure if that's Queue Name.");
              System.out.print ("\n\nPlease Clarify? \n1 => Press One to Exit \n2 =>  Press 2 Continue \n3 => Quit is my Queue Name\n\n Your Option :");
              ans=msgstream.readLine();
              choice=Integer.parseInt(ans);
              switch (choice)
              {
                  case 1: quitNow=true; 
                          System.out.println("GoodBye!");
                          System.exit(0);
                          break;
                  case 2: quitNow=false;
                          break;
                  case 3: quitNow=false;
                          sobj.init(QN);
                          sobj.receivemsg(1000, true);
                          sobj.closeConnection();
                          break;
                  default:
                          LOGGER.error("No Such Option Sorry! Try Again");
                          quitNow=false;
                          break;
                            
              }
          }
          else
          {
              sobj.init(QN);
              sobj.receivemsg(10, true);
              sobj.closeConnection();
              
          }
          }
          while(! quitNow);
          
          
          
      } catch (JMSException ex) {
          java.util.logging.Logger.getLogger(QueueReceiver.class.getName()).log(Level.SEVERE, null, ex);
      }
      } 
  }

