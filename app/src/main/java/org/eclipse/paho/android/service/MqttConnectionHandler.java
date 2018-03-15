package org.eclipse.paho.android.service;

import org.eclipse.paho.client.mqttv3.DisconnectedBufferOptions;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * @author Gaurav Dingolia
 */

public class MqttConnectionHandler
{
    private MqttConnectionManager mMqttConnectionManager;
    private String mConnectionString;

    public MqttConnectionHandler(MqttConnectionManager connectionManager, String connectionString)
    {
        mMqttConnectionManager = connectionManager;
        mConnectionString = connectionString;
        mMqttConnectionManager.setTraceCallbackId(connectionString);
    }


    public void connect(MqttConnectOptions mqttConnectOptions, String invocationContext, String activityToken) throws
            MqttException
    {
        mMqttConnectionManager.connect(mConnectionString, mqttConnectOptions, invocationContext, activityToken);
    }

    public void disconnect(String invocationContext, String activityToken) throws MqttException
    {
        mMqttConnectionManager.disconnect(mConnectionString, invocationContext, activityToken);
    }

    public void disconnect(long quiesceTimeout, String invocationContext, String activityToken) throws MqttException
    {
        mMqttConnectionManager.disconnect(mConnectionString, quiesceTimeout, invocationContext, activityToken);
    }

    public void close()
    {
        mMqttConnectionManager.close(mConnectionString);
    }

    public IMqttDeliveryToken publish(String topic, byte[] payload, int qos, boolean retained, String
            invocationContext, String activityToken) throws MqttException
    {
        return mMqttConnectionManager.publish(mConnectionString, topic, payload, qos, retained, invocationContext,
                activityToken);
    }

    public IMqttDeliveryToken publish(String topic, MqttMessage message, String
            invocationContext, String activityToken) throws MqttException
    {
        return mMqttConnectionManager.publish(mConnectionString, topic, message, invocationContext, activityToken);
    }

    public void subscribe(String topic, int qos, String invocationContext, String activityToken)
    {
        mMqttConnectionManager.subscribe(mConnectionString, topic, qos, invocationContext, activityToken);
    }

    public void subscribe(String[] topic, int[] qos, String invocationContext, String activityToken)
    {
        mMqttConnectionManager.subscribe(mConnectionString, topic, qos, invocationContext, activityToken);
    }

    public void subscribe(String[] topicFilters, int[] qos, String invocationContext, String
            activityToken, IMqttMessageListener[] messageListeners)
    {
        mMqttConnectionManager.subscribe(mConnectionString, topicFilters, qos, invocationContext, activityToken,
                messageListeners);
    }

    public void unsubscribe(final String topic, String invocationContext, String activityToken)
    {
        mMqttConnectionManager.unsubscribe(mConnectionString, topic, invocationContext, activityToken);
    }

    public void unsubscribe(final String[] topic, String invocationContext, String activityToken)
    {
        mMqttConnectionManager.unsubscribe(mConnectionString, topic, invocationContext, activityToken);
    }

    public IMqttDeliveryToken[] getPendingDeliveryTokens()
    {
        return mMqttConnectionManager.getPendingDeliveryTokens(mConnectionString);
    }

    public Status acknowledgeMessageArrival(String id)
    {
        return mMqttConnectionManager.acknowledgeMessageArrival(mConnectionString, id);
    }

    public int getBufferedMessageCount()
    {
        return mMqttConnectionManager.getBufferedMessageCount(mConnectionString);
    }

    public void setBufferOpts(DisconnectedBufferOptions bufferOpts)
    {
        mMqttConnectionManager.setBufferOpts(mConnectionString, bufferOpts);
    }

    public MqttMessage getBufferedMessage(int bufferIndex)
    {
        return mMqttConnectionManager.getBufferedMessage(mConnectionString, bufferIndex);
    }

    public void deleteBufferedMessage(int bufferIndex)
    {
        mMqttConnectionManager.deleteBufferedMessage(mConnectionString, bufferIndex);
    }

    public boolean isConnected()
    {
        return mMqttConnectionManager.isConnected(mConnectionString);
    }

    public void traceError(String tag, String message)
    {
        mMqttConnectionManager.traceError(tag, message);
    }

    public void setTraceEnabled(boolean traceEnabled)
    {
        mMqttConnectionManager.setTraceEnabled(traceEnabled);
    }


}
