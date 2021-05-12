package com.manning.lp.cloud.iot.endtoend;

import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Properties;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.joda.time.DateTime;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Random;
import java.sql.Timestamp;

public class CloudiotPubsubExampleMqttDevice {

  /** Create a RSA-based JWT for the given project id, signed with the given private key. */
  private static String createJwtRsa(String projectId, String privateKeyFile) throws Exception {
    DateTime now = new DateTime();
    // Create a JWT to authenticate this device. The device will be disconnected after the token
    // expires, and will have to reconnect with a new token. The audience field should always be set
    // to the GCP project id.
    JwtBuilder jwtBuilder =
        Jwts.builder()
            .setIssuedAt(now.toDate())
            .setExpiration(now.plusMinutes(20).toDate())
            .setAudience(projectId);

    byte[] keyBytes = Files.readAllBytes(Paths.get(privateKeyFile));
    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
    KeyFactory kf = KeyFactory.getInstance("RSA");

    return jwtBuilder.signWith(SignatureAlgorithm.RS256, kf.generatePrivate(spec)).compact();
  }

  /** Create an ES-based JWT for the given project id, signed with the given private key. */
  private static String createJwtEs(String projectId, String privateKeyFile) throws Exception {
    DateTime now = new DateTime();
    // Create a JWT to authenticate this device. The device will be disconnected after the token
    // expires, and will have to reconnect with a new token. The audience field should always be set
    // to the GCP project id.
    JwtBuilder jwtBuilder =
        Jwts.builder()
            .setIssuedAt(now.toDate())
            .setExpiration(now.plusMinutes(20).toDate())
            .setAudience(projectId);

    byte[] keyBytes = Files.readAllBytes(Paths.get(privateKeyFile));
    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
    KeyFactory kf = KeyFactory.getInstance("EC");

    return jwtBuilder.signWith(SignatureAlgorithm.ES256, kf.generatePrivate(spec)).compact();
  }

  /** Represents the state of a single device. */
  static class Device implements MqttCallback {
    private int temperature;
    private boolean isFanOn;
    private boolean isConnected;
    private Timestamp timestamp;
    private int tsunami_event_validity;
    private int tsunami_cause_code;
    private float earthquake_magnitude;    
    private float latitude;
    private float longitude;
    private float maximum_water_height;

    public Device(CloudiotPubsubExampleMqttDeviceOptions options) {
      this.temperature = 0;
      this.isFanOn = false;
      this.isConnected = false;
   }

    /**
     * Pretend to read the device's sensor data. If the fan is on, assume the temperature decreased
     * one degree, otherwise assume that it increased one degree.
     */
    public void generateSensorData() {
      Random  random = new Random();
      long start2018 = 1514745000000L;
      final long millisInYear2018 = 1000 * 60 * 60 * 24 * 365 + 1000;
      long millis = Math.round(millisInYear2018 * Math.random());
      this.timestamp = new Timestamp(start2018 + millis);

      this.tsunami_event_validity = random.nextInt(10);
      this.tsunami_cause_code = random.nextInt(10);
      float minEarthQuakeMagnitude = 5.00F;
      float maxEarthQuakeMagnitude = 10.00F;
      this.earthquake_magnitude = random.nextFloat() * (maxEarthQuakeMagnitude - minEarthQuakeMagnitude) + minEarthQuakeMagnitude;

      float minLat = -90.00F;
      float maxLat = 90.00F;
      this.latitude = minLat + (float)(Math.random() * ((maxLat - minLat) + 1));
      float minLon = -180.00F;
      float maxLon = 180.00F;
      this.longitude = minLon + (float)(Math.random() * ((maxLon - minLon) + 1));

      float minWaterHeight = 0.5F;
      float maxWaterHeight = 50.00F;
      this.maximum_water_height = random.nextFloat() * (maxWaterHeight - minWaterHeight ) + minWaterHeight;

    }

    /** Wait for the device to become connected. */
    public void waitForConnection(int timeOut) throws InterruptedException {
      // Wait for the device to become connected.
      int totalTime = 0;
      while (!this.isConnected && totalTime < timeOut) {
        Thread.sleep(1000);
        totalTime += 1;
      }

      if (!this.isConnected) {
        throw new RuntimeException("Could not connect to MQTT bridge.");
      }
    }

    /** Callback when the device receives a PUBACK from the MQTT bridge. */
    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
      System.out.println("Published message acked.");
    }

    /** Callback when the device receives a message on a subscription. */
    @Override
    public void messageArrived(String topic, MqttMessage message) {
      String payload = new String(message.getPayload());
      System.out.println(
          String.format(
              "Received message %s on topic %s with Qos %d", payload, topic, message.getQos()));

      // The device will receive its latest config when it subscribes to the
      // config topic. If there is no configuration for the device, the device
      // will receive a config with an empty payload.
      if (payload == null || payload.length() == 0) {
        return;
      }

      // The config is passed in the payload of the message. In this example,
      // the server sends a serialized JSON string.
      JSONObject data = null;
      try {
        data = new JSONObject(payload);
        if (data.get("fan_on") instanceof Boolean && (Boolean) data.get("fan_on") != this.isFanOn) {
          // If changing the state of the fan, print a message and
          // update the internal state.
          this.isFanOn = (Boolean) data.get("fan_on");
          if (this.isFanOn) {
            System.out.println("Fan turned on");
          } else {
            System.out.println("Fan turned off");
          }
        }
      } catch (JSONException e) {
        e.printStackTrace();
      }
    }

    /** Callback for when a device disconnects. */
    @Override
    public void connectionLost(Throwable cause) {
      System.out.println("Disconnected: " + cause.getMessage());
      this.isConnected = false;
    }
  }

  /** Entry point for CLI. */
  public static void main(String[] args) throws Exception {
    CloudiotPubsubExampleMqttDeviceOptions options =
        CloudiotPubsubExampleMqttDeviceOptions.fromFlags(args);
    if (options == null) {
      System.exit(1);
    }
    final Device device = new Device(options);
    final String mqttTelemetryTopic = String.format("/devices/%s/events", options.deviceId);
    // This is the topic that the device will receive configuration updates on.
    final String mqttConfigTopic = String.format("/devices/%s/config", options.deviceId);

    final String mqttServerAddress =
        String.format("ssl://%s:%s", options.mqttBridgeHostname, options.mqttBridgePort);
    final String mqttClientId =
        String.format(
            "projects/%s/locations/%s/registries/%s/devices/%s",
            options.projectId, options.cloudRegion, options.registryId, options.deviceId);
    MqttConnectOptions connectOptions = new MqttConnectOptions();
    connectOptions.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);

    Properties sslProps = new Properties();
    sslProps.setProperty("com.ibm.ssl.protocol", "TLSv1.2");
    connectOptions.setSSLProperties(sslProps);

    connectOptions.setUserName("unused");
    if (options.algorithm.equals("RS256")) {
      System.out.println(options.privateKeyFile);
      System.out.println("************** Welcome to Manning Live Project Data Streaming Pipeline Implementation *******************");

      connectOptions.setPassword(
          createJwtRsa(options.projectId, options.privateKeyFile).toCharArray());
      System.out.println(
          String.format(
              "Creating JWT using RS256 from private key file %s", options.privateKeyFile));
    } else if (options.algorithm.equals("ES256")) {
      connectOptions.setPassword(
          createJwtEs(options.projectId, options.privateKeyFile).toCharArray());
    } else {
      throw new IllegalArgumentException(
          "Invalid algorithm " + options.algorithm + ". Should be one of 'RS256' or 'ES256'.");
    }

    device.isConnected = true;

    MqttClient client = new MqttClient(mqttServerAddress, mqttClientId, new MemoryPersistence());

    try {
      client.setCallback(device);
      client.connect(connectOptions);
    } catch (MqttException e) {
      e.printStackTrace();
    }

    // wait for it to connect
    device.waitForConnection(5);

    client.subscribe(mqttConfigTopic, 1);

    for (int i = 0; i < options.numMessages; i++) {
      device.generateSensorData();

      JSONObject payload = new JSONObject();
      payload.put("timestamp", device.timestamp);
      payload.put("tsunami_event_validity", device.tsunami_event_validity);
      payload.put("tsunami_cause_code", device.tsunami_cause_code);
      payload.put("earthquake_magnitude", device.earthquake_magnitude);
      payload.put("latitude", device.latitude);
      payload.put("longitude", device.longitude);
      payload.put("maximum_water_height",device.maximum_water_height); 

      System.out.println("Publishing payload " + payload.toString());
      MqttMessage message = new MqttMessage(payload.toString().getBytes());
      message.setQos(1);
      client.publish(mqttTelemetryTopic, message);
      Thread.sleep(1000);
    }
    client.disconnect();

    System.out.println("Finished looping successfully : " + options.mqttBridgeHostname);
  }
}
