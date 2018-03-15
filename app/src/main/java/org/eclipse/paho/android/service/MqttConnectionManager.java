package org.eclipse.paho.android.service;

import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.support.v4.content.LocalBroadcastManager;

import org.eclipse.paho.client.mqttv3.DisconnectedBufferOptions;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Gaurav Dingolia
 */

public class MqttConnectionManager implements MqttTraceHandler
{
    private static final String TAG = MqttConnectionManager.class.getSimpleName();
    private Context mContext;
    // somewhere to persist received messages until we're sure
    // that they've reached the application
    private MessageStore mMessageStore;
    // callback id for making trace callbacks to the Activity
    // needs to be set by the activity as appropriate
    private String traceCallbackId;
    // state of tracing
    private boolean traceEnabled = false;
    private Map<String, MqttConnection> connections = new ConcurrentHashMap<>();
    private MqttConnection.Callback mCallback;

    //TODO: should it be singleton?
    public MqttConnectionManager(Context context)
    {
        mContext = context;
        mMessageStore = new DatabaseMessageStore(this, context);
        mCallback = new LocalBroadcastCallback(context);
    }

    /**
     * Get an MqttConnection object to represent a connection to a server
     *
     * @param serverURI   specifies the protocol, host name and port to be used to connect to an MQTT server
     * @param clientId    specifies the name by which this connection should be identified to the server
     * @param contextId   specifies the app context info to make a difference between apps
     * @param persistence specifies the persistence layer to be used with this client
     * @return a string to be used by the Activity as a "handle" for this
     * MqttConnection
     */
    public String getConnectionString(String serverURI, String clientId, String contextId, MqttClientPersistence
            persistence)
    {
        String connectionString = serverURI + ":" + clientId + ":" + contextId;
        if (!connections.containsKey(connectionString))
        {
            MqttConnection client = new MqttConnection(mContext, mMessageStore, serverURI, clientId, persistence,
                    connectionString, mCallback);
            connections.put(connectionString, client);
        }
        return connectionString;
    }

    // The major API implementation follows :-

    /**
     * Connect to the MQTT server specified by a particular client
     *
     * @param clientHandle      identifies the MqttConnection to use
     * @param connectOptions    the MQTT connection options to be used
     * @param invocationContext arbitrary data to be passed back to the application
     * @param activityToken     arbitrary identifier to be passed back to the Activity
     * @throws MqttSecurityException thrown if there is a security exception
     * @throws MqttException         thrown for all other MqttExceptions
     */
    public void connect(String clientHandle, MqttConnectOptions connectOptions, String invocationContext, String
            activityToken) throws MqttSecurityException, MqttException
    {
        MqttConnection client = getConnection(clientHandle);
        client.connect(connectOptions, invocationContext, activityToken);

    }

    /**
     * Request all clients to reconnect if appropriate
     */
    public void reconnect()
    {
        traceDebug(TAG, "Reconnect to server, client size=" + connections.size());
        for (MqttConnection client : connections.values())
        {
            traceDebug("Reconnect Client:", client.getClientId() + '/' + client.getServerURI());
            if (Utils.isOnline(mContext)) client.reconnect();
        }
    }

    /**
     * Close connection from a particular client
     *
     * @param clientHandle identifies the MqttConnection to use
     */
    public void close(String clientHandle)
    {
        MqttConnection client = getConnection(clientHandle);
        client.close();
    }

    /**
     * Disconnect from the server
     *
     * @param clientHandle      identifies the MqttConnection to use
     * @param invocationContext arbitrary data to be passed back to the application
     * @param activityToken     arbitrary identifier to be passed back to the Activity
     */
    public void disconnect(String clientHandle, String invocationContext,
            String activityToken)
    {
        MqttConnection client = getConnection(clientHandle);
        client.disconnect(invocationContext, activityToken);
        connections.remove(clientHandle);
    }

    /**
     * Disconnect from the server
     *
     * @param clientHandle      identifies the MqttConnection to use
     * @param quiesceTimeout    in milliseconds
     * @param invocationContext arbitrary data to be passed back to the application
     * @param activityToken     arbitrary identifier to be passed back to the Activity
     */
    public void disconnect(String clientHandle, long quiesceTimeout, String invocationContext, String activityToken)
    {
        MqttConnection client = getConnection(clientHandle);
        client.disconnect(quiesceTimeout, invocationContext, activityToken);
        connections.remove(clientHandle);
    }

    /**
     * Get the status of a specific client
     *
     * @param clientHandle identifies the MqttConnection to use
     * @return true if the specified client is connected to an MQTT server
     */
    public boolean isConnected(String clientHandle)
    {
        MqttConnection client = getConnection(clientHandle);
        return client.isConnected();
    }

    /**
     * Publish a message to a topic
     *
     * @param clientHandle      identifies the MqttConnection to use
     * @param topic             the topic to which to publish
     * @param payload           the content of the message to publish
     * @param qos               the quality of service requested
     * @param retained          whether the MQTT server should retain this message
     * @param invocationContext arbitrary data to be passed back to the application
     * @param activityToken     arbitrary identifier to be passed back to the Activity
     * @return token for tracking the operation
     * @throws MqttPersistenceException when a problem occurs storing the message
     * @throws MqttException            if there was an error publishing the message
     */
    public IMqttDeliveryToken publish(String clientHandle, String topic, byte[] payload, int qos, boolean retained,
            String invocationContext, String activityToken) throws MqttPersistenceException, MqttException
    {
        MqttConnection client = getConnection(clientHandle);
        return client.publish(topic, payload, qos, retained, invocationContext,
                activityToken);
    }

    /**
     * Publish a message to a topic
     *
     * @param clientHandle      identifies the MqttConnection to use
     * @param topic             the topic to which to publish
     * @param message           the message to publish
     * @param invocationContext arbitrary data to be passed back to the application
     * @param activityToken     arbitrary identifier to be passed back to the Activity
     * @return token for tracking the operation
     * @throws MqttPersistenceException when a problem occurs storing the message
     * @throws MqttException            if there was an error publishing the message
     */
    public IMqttDeliveryToken publish(String clientHandle, String topic, MqttMessage message, String
            invocationContext, String activityToken) throws MqttPersistenceException, MqttException
    {
        MqttConnection client = getConnection(clientHandle);
        return client.publish(topic, message, invocationContext, activityToken);
    }

    /**
     * Subscribe to a topic
     *
     * @param clientHandle      identifies the MqttConnection to use
     * @param topic             a possibly wildcarded topic name
     * @param qos               requested quality of service for the topic
     * @param invocationContext arbitrary data to be passed back to the application
     * @param activityToken     arbitrary identifier to be passed back to the Activity
     */
    public void subscribe(String clientHandle, String topic, int qos, String invocationContext, String activityToken)
    {
        MqttConnection client = getConnection(clientHandle);
        client.subscribe(topic, qos, invocationContext, activityToken);
    }

    /**
     * Subscribe to one or more topics
     *
     * @param clientHandle      identifies the MqttConnection to use
     * @param topic             a list of possibly wildcarded topic names
     * @param qos               requested quality of service for each topic
     * @param invocationContext arbitrary data to be passed back to the application
     * @param activityToken     arbitrary identifier to be passed back to the Activity
     */
    public void subscribe(String clientHandle, String[] topic, int[] qos, String invocationContext, String
            activityToken)
    {
        MqttConnection client = getConnection(clientHandle);
        client.subscribe(topic, qos, invocationContext, activityToken);
    }

    /**
     * Subscribe using topic filters
     *
     * @param clientHandle      identifies the MqttConnection to use
     * @param topicFilters      a list of possibly wildcarded topicfilters
     * @param qos               requested quality of service for each topic
     * @param invocationContext arbitrary data to be passed back to the application
     * @param activityToken     arbitrary identifier to be passed back to the Activity
     * @param messageListeners  a callback to handle incoming messages
     */
    public void subscribe(String clientHandle, String[] topicFilters, int[] qos, String invocationContext, String
            activityToken, IMqttMessageListener[] messageListeners)
    {
        MqttConnection client = getConnection(clientHandle);
        client.subscribe(topicFilters, qos, invocationContext, activityToken, messageListeners);
    }

    /**
     * Unsubscribe from a topic
     *
     * @param clientHandle      identifies the MqttConnection
     * @param topic             a possibly wildcarded topic name
     * @param invocationContext arbitrary data to be passed back to the application
     * @param activityToken     arbitrary identifier to be passed back to the Activity
     */
    public void unsubscribe(String clientHandle, final String topic, String invocationContext, String activityToken)
    {
        MqttConnection client = getConnection(clientHandle);
        client.unsubscribe(topic, invocationContext, activityToken);
    }

    /**
     * Unsubscribe from one or more topics
     *
     * @param clientHandle      identifies the MqttConnection
     * @param topic             a list of possibly wildcarded topic names
     * @param invocationContext arbitrary data to be passed back to the application
     * @param activityToken     arbitrary identifier to be passed back to the Activity
     */
    public void unsubscribe(String clientHandle, final String[] topic, String invocationContext, String activityToken)
    {
        MqttConnection client = getConnection(clientHandle);
        client.unsubscribe(topic, invocationContext, activityToken);
    }

    /**
     * Get tokens for all outstanding deliveries for a client
     *
     * @param clientHandle identifies the MqttConnection
     * @return an array (possibly empty) of tokens
     */
    public IMqttDeliveryToken[] getPendingDeliveryTokens(String clientHandle)
    {
        MqttConnection client = getConnection(clientHandle);
        return client.getPendingDeliveryTokens();
    }

    /**
     * Get the MqttConnection identified by this client handle
     *
     * @param clientHandle identifies the MqttConnection
     * @return the MqttConnection identified by this handle
     */
    private MqttConnection getConnection(String clientHandle)
    {
        MqttConnection client = connections.get(clientHandle);
        if (client == null)
        {
            throw new IllegalArgumentException("Invalid ClientHandle");
        }
        return client;
    }

    /**
     * Called by the Activity when a message has been passed back to the
     * application
     *
     * @param clientHandle identifier for the client which received the message
     * @param id           identifier for the MQTT message
     * @return {@link Status}
     */
    public Status acknowledgeMessageArrival(String clientHandle, String id)
    {
        if (mMessageStore.discardArrived(clientHandle, id))
        {
            return Status.OK;
        }
        else
        {
            return Status.ERROR;
        }
    }

    /**
     * Identify the callbackId to be passed when making tracing calls back into
     * the Activity
     *
     * @param traceCallbackId identifier to the callback into the Activity
     */
    public void setTraceCallbackId(String traceCallbackId)
    {
        this.traceCallbackId = traceCallbackId;
    }

    /**
     * Check whether trace is on or off.
     *
     * @return the state of trace
     */
    public boolean isTraceEnabled()
    {
        return this.traceEnabled;
    }

    /**
     * Turn tracing on and off
     *
     * @param traceEnabled set <code>true</code> to turn on tracing, <code>false</code> to turn off tracing
     */
    public void setTraceEnabled(boolean traceEnabled)
    {
        this.traceEnabled = traceEnabled;
    }

    /**
     * Trace debugging information
     *
     * @param tag     identifier for the source of the trace
     * @param message the text to be traced
     */
    @Override
    public void traceDebug(String tag, String message)
    {
        traceCallback(MqttServiceConstants.TRACE_DEBUG, tag, message);
    }

    /**
     * Trace error information
     *
     * @param tag     identifier for the source of the trace
     * @param message the text to be traced
     */
    @Override
    public void traceError(String tag, String message)
    {
        traceCallback(MqttServiceConstants.TRACE_ERROR, tag, message);
    }

    private void traceCallback(String severity, String tag, String message)
    {
        if ((traceCallbackId != null) && (traceEnabled))
        {
            Bundle dataBundle = new Bundle();
            dataBundle.putString(MqttServiceConstants.CALLBACK_ACTION, MqttServiceConstants.TRACE_ACTION);
            dataBundle.putString(MqttServiceConstants.CALLBACK_TRACE_SEVERITY, severity);
            dataBundle.putString(MqttServiceConstants.CALLBACK_TRACE_TAG, tag);
            //dataBundle.putString(MqttServiceConstants.CALLBACK_TRACE_ID, traceCallbackId);
            dataBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE, message);
            broadcastEvent(mContext, traceCallbackId, Status.ERROR, dataBundle);
        }
    }

    /**
     * trace exceptions
     *
     * @param tag     identifier for the source of the trace
     * @param message the text to be traced
     * @param e       the exception
     */
    @Override
    public void traceException(String tag, String message, Exception e)
    {
        if (traceCallbackId != null)
        {
            Bundle dataBundle = new Bundle();
            dataBundle.putString(MqttServiceConstants.CALLBACK_ACTION, MqttServiceConstants.TRACE_ACTION);
            dataBundle.putString(MqttServiceConstants.CALLBACK_TRACE_SEVERITY, MqttServiceConstants.TRACE_EXCEPTION);
            dataBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE, message);
            dataBundle.putSerializable(MqttServiceConstants.CALLBACK_EXCEPTION, e); //TODO: Check
            dataBundle.putString(MqttServiceConstants.CALLBACK_TRACE_TAG, tag);
            //dataBundle.putString(MqttServiceConstants.CALLBACK_TRACE_ID, traceCallbackId);
            broadcastEvent(mContext, traceCallbackId, Status.ERROR, dataBundle);
        }
    }


    /**
     * Notify clients we're offline
     */
    public void notifyClientsOffline()
    {
        for (MqttConnection connection : connections.values())
        {
            connection.offline();
        }
    }

    /**
     * Sets the DisconnectedBufferOptions for this client
     *
     * @param clientHandle identifier for the client
     * @param bufferOpts   the DisconnectedBufferOptions for this client
     */
    public void setBufferOpts(String clientHandle, DisconnectedBufferOptions bufferOpts)
    {
        MqttConnection client = getConnection(clientHandle);
        client.setBufferOpts(bufferOpts);
    }

    public int getBufferedMessageCount(String clientHandle)
    {
        MqttConnection client = getConnection(clientHandle);
        return client.getBufferedMessageCount();
    }

    public MqttMessage getBufferedMessage(String clientHandle, int bufferIndex)
    {
        MqttConnection client = getConnection(clientHandle);
        return client.getBufferedMessage(bufferIndex);
    }

    public void deleteBufferedMessage(String clientHandle, int bufferIndex)
    {
        MqttConnection client = getConnection(clientHandle);
        client.deleteBufferedMessage(bufferIndex);
    }

    public void finish()
    {
        mMessageStore.close();
    }

    /**
     * pass data back to the Activity, by building a suitable Intent object and
     * broadcasting it
     *
     * @param clientHandle source of the data
     * @param status       OK or Error
     * @param dataBundle   the data to be passed
     */
    private static void broadcastEvent(Context context, String clientHandle, Status status, Bundle dataBundle)
    {
        // Don't call traceDebug, as it will try to broadcastEvent leading
        // to recursion.
        Intent callbackIntent = new Intent(MqttServiceConstants.CALLBACK_TO_ACTIVITY);
        if (clientHandle != null)
        {
            callbackIntent.putExtra(MqttServiceConstants.CALLBACK_CLIENT_HANDLE, clientHandle);
        }
        callbackIntent.putExtra(MqttServiceConstants.CALLBACK_STATUS, status);
        if (dataBundle != null)
        {
            callbackIntent.putExtras(dataBundle);
        }
        LocalBroadcastManager.getInstance(context).sendBroadcast(callbackIntent);
    }

    private static class LocalBroadcastCallback implements MqttConnection.Callback
    {
        private Context mContext;

        LocalBroadcastCallback(Context mContext)
        {
            this.mContext = mContext.getApplicationContext();
        }

        @Override
        public void onEvent(String clientHandle, Status status, Bundle dataBundle)
        {
            broadcastEvent(mContext, clientHandle, status, dataBundle);
        }
    }

}
