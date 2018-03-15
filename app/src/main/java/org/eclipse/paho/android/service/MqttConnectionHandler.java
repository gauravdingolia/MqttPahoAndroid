package org.eclipse.paho.android.service;

import android.text.TextUtils;

import org.eclipse.paho.client.mqttv3.DisconnectedBufferOptions;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
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

    public MqttConnectionHandler(MqttConnectionManager connectionManager)
    {
        mMqttConnectionManager = connectionManager;
    }

    public void initConnection(String serverURI, String clientId, String contextId, MqttClientPersistence persistence)
    {
        if (isConnectionInitialised()) throw new IllegalStateException("Connection already initialized");

        if (TextUtils.isEmpty(serverURI))
            throw new NullPointerException("Server URI cannot be null/empty");
        if (TextUtils.isEmpty(clientId))
            throw new NullPointerException("Client id cannot be null/empty");
        if (TextUtils.isEmpty(contextId))
            throw new NullPointerException("Context id cannot be null/empty");
        mConnectionString = mMqttConnectionManager.getConnectionString(serverURI, clientId, contextId, persistence);
        mMqttConnectionManager.setTraceCallbackId(mConnectionString);
    }

    public void connect(MqttConnectOptions mqttConnectOptions, String invocationContext, String activityToken) throws
            MqttException
    {
        checkInitialisationOrThrow();
        mMqttConnectionManager.connect(mConnectionString, mqttConnectOptions, invocationContext, activityToken);
    }

    public void disconnect(String invocationContext, String activityToken) throws MqttException
    {
        checkInitialisationOrThrow();
        mMqttConnectionManager.disconnect(mConnectionString, invocationContext, activityToken);
    }

    public void disconnect(long quiesceTimeout, String invocationContext, String activityToken) throws MqttException
    {
        checkInitialisationOrThrow();
        mMqttConnectionManager.disconnect(mConnectionString, quiesceTimeout, invocationContext, activityToken);
    }

    public void close()
    {
        if (!isConnectionInitialised()) return;
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
        checkInitialisationOrThrow();
        return mMqttConnectionManager.publish(mConnectionString, topic, message, invocationContext, activityToken);
    }

    public void subscribe(String topic, int qos, String invocationContext, String activityToken)
    {
        checkInitialisationOrThrow();
        mMqttConnectionManager.subscribe(mConnectionString, topic, qos, invocationContext, activityToken);
    }

    public void subscribe(String[] topic, int[] qos, String invocationContext, String activityToken)
    {
        checkInitialisationOrThrow();
        mMqttConnectionManager.subscribe(mConnectionString, topic, qos, invocationContext, activityToken);
    }

    public void subscribe(String[] topicFilters, int[] qos, String invocationContext, String
            activityToken, IMqttMessageListener[] messageListeners)
    {
        checkInitialisationOrThrow();
        mMqttConnectionManager.subscribe(mConnectionString, topicFilters, qos, invocationContext, activityToken,
                messageListeners);
    }

    public void unsubscribe(final String topic, String invocationContext, String activityToken)
    {
        checkInitialisationOrThrow();
        mMqttConnectionManager.unsubscribe(mConnectionString, topic, invocationContext, activityToken);
    }

    public void unsubscribe(final String[] topic, String invocationContext, String activityToken)
    {
        checkInitialisationOrThrow();
        mMqttConnectionManager.unsubscribe(mConnectionString, topic, invocationContext, activityToken);
    }

    public IMqttDeliveryToken[] getPendingDeliveryTokens()
    {
        checkInitialisationOrThrow();
        return mMqttConnectionManager.getPendingDeliveryTokens(mConnectionString);
    }

    public Status acknowledgeMessageArrival(String id)
    {
        checkInitialisationOrThrow();
        return mMqttConnectionManager.acknowledgeMessageArrival(mConnectionString, id);
    }

    public int getBufferedMessageCount()
    {
        checkInitialisationOrThrow();
        return mMqttConnectionManager.getBufferedMessageCount(mConnectionString);
    }

    public void setBufferOpts(DisconnectedBufferOptions bufferOpts)
    {
        checkInitialisationOrThrow();
        mMqttConnectionManager.setBufferOpts(mConnectionString, bufferOpts);
    }

    public MqttMessage getBufferedMessage(int bufferIndex)
    {
        checkInitialisationOrThrow();
        return mMqttConnectionManager.getBufferedMessage(mConnectionString, bufferIndex);
    }

    public void deleteBufferedMessage(int bufferIndex)
    {
        checkInitialisationOrThrow();
        mMqttConnectionManager.deleteBufferedMessage(mConnectionString, bufferIndex);
    }

    public boolean isConnected()
    {
        return isConnectionInitialised() && mMqttConnectionManager.isConnected(mConnectionString);
    }

    private boolean isConnectionInitialised()
    {
        return mConnectionString != null;
    }

    private void checkInitialisationOrThrow()
    {
        if (!isConnectionInitialised())
            throw new IllegalStateException("Connection not initialised");
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
