package com.pbakhil.iot.app;

import com.microsoft.azure.sdk.iot.device.*;
import com.google.gson.Gson;

import java.io.*;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class IOTDeviceClientSimulator {

	private static final String IOTHUBNAME = "pbakhiliothub1";
	private static final String DEVICEID = "FirstJavaDevice";
	//private static final String DEVICEKEY = "nfzLTSR7eKr7vcEcgQ9Q01f89YhN6f6ism4lEqKYj80=";
	private static final String DEVICEKEY = "GljxkZ50xJVNVE8UFRYnkw==";
	

	private static String connString = "HostName=" + IOTHUBNAME + ".azure-devices.net;DeviceId=" + DEVICEID
			+ ";SharedAccessKey={" + DEVICEKEY + "}";
	private static IotHubClientProtocol protocol = IotHubClientProtocol.MQTT;
	private static String deviceId = "FirstJavaDevice";
	private static DeviceClient client;

	public static void main(String[] args) throws IOException, URISyntaxException {
		client = new DeviceClient(connString, protocol);
		client.open();

		MessageSender sender = new MessageSender();

		ExecutorService executor = Executors.newFixedThreadPool(1);
		executor.execute(sender);

		System.out.println("Press ENTER to exit.");
		System.in.read();
		executor.shutdownNow();
		client.closeNow();
	}

	private static class TelemetryDataPoint {
		public String deviceId;
		public double temperature;
		public double humidity;

		public String serialize() {
			Gson gson = new Gson();
			return gson.toJson(this);
		}
	}

	private static class EventCallback implements IotHubEventCallback {
		public void execute(IotHubStatusCode status, Object context) {
			System.out.println("IoT Hub responded to message with status: " + status.name());

			if (context != null) {
				synchronized (context) {
					context.notify();
				}
			}
		}
	}

	private static class MessageSender implements Runnable {
		public void run() {
			try {
				double minTemperature = 20;
				double minHumidity = 60;
				Random rand = new Random();

				while (true) {
					double currentTemperature = minTemperature + rand.nextDouble() * 15;
					double currentHumidity = minHumidity + rand.nextDouble() * 20;
					TelemetryDataPoint telemetryDataPoint = new TelemetryDataPoint();
					telemetryDataPoint.deviceId = deviceId;
					telemetryDataPoint.temperature = currentTemperature;
					telemetryDataPoint.humidity = currentHumidity;

					String msgStr = telemetryDataPoint.serialize();
					Message msg = new Message(msgStr);
					msg.setProperty("temperatureAlert", (currentTemperature > 30) ? "true" : "false");
					msg.setMessageId(java.util.UUID.randomUUID().toString());
					System.out.println("Sending: " + msgStr);

					Object lockobj = new Object();
					EventCallback callback = new EventCallback();
					client.sendEventAsync(msg, callback, lockobj);

					synchronized (lockobj) {
						lockobj.wait();
					}
					Thread.sleep(1000);
				}
			} catch (InterruptedException e) {
				System.out.println("Finished.");
			}
		}
	}

}
