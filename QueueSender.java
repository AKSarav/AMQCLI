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
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueSender {
   
    private static final Logger LOGGER =
      LoggerFactory.getLogger(QueueSender.class);

  //private String clientId;
  private Connection connection;
  private Session session;
  private MessageProducer messageProducer;
  String un="";
  String pw="";
  String url="";
  private static String QN="";
  private static BufferedReader msgstream = new BufferedReader(new InputStreamReader(System.in));
  
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
    
    }   

    
    // create a Session
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

    //System.out.print ("what is session"+session);

    // create the Queue to which messages will be sent . If the Queue is not there it will be auto created
    Queue queue = session.createQueue(queueName);

    // create a MessageProducer for sending messages
    messageProducer = session.createProducer(queue);
  }

  public void closeConnection() throws JMSException {
    connection.close();
  }

  public void sendmessage(String Message)
      throws JMSException {
      
    String text = Message;

    // create a JMS TextMessage
    TextMessage textMessage = session.createTextMessage(text);

    // send the message to the queue destination
    messageProducer.send(textMessage);

    
    LOGGER.info("sent message with text='{}'", text);
  }

  
  public static void main(String[] args) throws IOException, JMSException, URISyntaxException
  {
      
      System.out.println("-------------------------------");
      System.out.println("ACTIVEMQ - QUEUE SENDER - CLI");
      System.out.println("-------------------------------");
      String line="";
      QueueSender sobj = new QueueSender();
      BufferedReader msgStream = new BufferedReader(new InputStreamReader(System.in));
      
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
              System.out.print ("\n\nPlease Clarify? \n1 => Press One to Exit \n2 =>  Press 2 to Reset/Continue \n3 => Quit is my Queue Name\n\nYour Option :");
              ans=msgStream.readLine();
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
                          System.out.print("\nEnter the message to send: \n");
                          line = msgStream.readLine();
                          sobj.sendmessage(line);
                          LOGGER.info("JMS Message Sent: "+line+"\n");
                          
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
              System.out.print("\nEnter the message to send: \n");
              line = msgStream.readLine();
              sobj.sendmessage(line);
              sobj.closeConnection();
              
          }
          }
          while(! quitNow);
          
        
          
          sobj.closeConnection();
      } catch (JMSException ex) {
          java.util.logging.Logger.getLogger(QueueSender.class.getName()).log(Level.SEVERE, null, ex);
      }
  }
}