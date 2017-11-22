package com.pbakhil.iot.app;

import java.io.IOException;
import com.microsoft.azure.eventhubs.*;
import com.microsoft.azure.servicebus.*;

import java.nio.charset.Charset;
import java.time.*;
import java.util.function.*;

public class IOTMessageReader {

	private final static String EVENTHUBCOMPATIBLENAME = "iothub-ehub-pbakhiliot-217452-8f997ee8d8";
	private final static String EVENTHUBCOMPATIBLEENDPOINT = "sb://ihsuproddbres027dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=nfzLTSR7eKr7vcEcgQ9Q01f89YhN6f6ism4lEqKYj80=";
	private final static String IOTHUBKEY = "nfzLTSR7eKr7vcEcgQ9Q01f89YhN6f6ism4lEqKYj80=";

	private static String connStr = "Endpoint="+ EVENTHUBCOMPATIBLEENDPOINT +";EntityPath="+ EVENTHUBCOMPATIBLENAME+ ";SharedAccessKeyName=iothubowner;SharedAccessKey="+ IOTHUBKEY +"";
	
	public static void main(String[] args) {
		// Create receivers for partitions 0 and 1.
		EventHubClient client0 = receiveMessages("0");
		EventHubClient client1 = receiveMessages("1");
		System.out.println("Press ENTER to exit.");
		try {
			System.in.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
		  client0.closeSync();
		  client1.closeSync();
		  System.exit(0);
		} catch (ServiceBusException sbe) {
		  System.exit(1);
		}
	}

	
	// Create a receiver on a partition.
	private static EventHubClient receiveMessages(final String partitionId) {
	  EventHubClient client = null;
	  try {
		  System.out.println(connStr);
	    client = EventHubClient.createFromConnectionStringSync(connStr);
	   
	  } catch (Exception e) {
	    System.out.println("Failed to create client: " + e.getMessage());
	    System.exit(1);
	  }
	  try {
	    // Create a receiver using the
	    // default Event Hubs consumer group
	    // that listens for messages from now on.
		  System.out.println("receive message1 : "+ client.createReceiver(EventHubClient.DEFAULT_CONSUMER_GROUP_NAME, partitionId, Instant.now().toString()));
	    client.createReceiver(EventHubClient.DEFAULT_CONSUMER_GROUP_NAME, partitionId, Instant.now())
	      .thenAccept(new Consumer<PartitionReceiver>() {
	        public void accept(PartitionReceiver receiver) {
	          System.out.println("** Created receiver on partition " + partitionId);
	          try {
	            while (true) {
	              Iterable<EventData> receivedEvents = receiver.receive(100).get();
	              int batchSize = 0;
	              if (receivedEvents != null) {
	                System.out.println("Got some evenst");
	                for (EventData receivedEvent : receivedEvents) {
	                  System.out.println(String.format("Offset: %s, SeqNo: %s, EnqueueTime: %s",
	                    receivedEvent.getSystemProperties().getOffset(),
	                    receivedEvent.getSystemProperties().getSequenceNumber(),
	                    receivedEvent.getSystemProperties().getEnqueuedTime()));
	                  System.out.println(String.format("| Device ID: %s",
	                    receivedEvent.getSystemProperties().get("iothub-connection-device-id")));
	                  System.out.println(String.format("| Message Payload: %s",
	                    new String(receivedEvent.getBytes(), Charset.defaultCharset())));
	                  batchSize++;
	                }
	              }
	              System.out.println(String.format("Partition: %s, ReceivedBatch Size: %s", partitionId, batchSize));
	            }
	          } catch (Exception e) {
	            System.out.println("Failed to receive messages: " + e.getMessage());
	          }
	        }
	      });
	    } catch (Exception e) {
	      System.out.println("Failed to create receiver: " + e.getMessage());
	  }
	  return client;
	}
}
