/*******************************************************************************
 * Copyright (c) 1999, 2016 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution. 
 *
 * The Eclipse Public License is available at 
 *    http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at 
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 */
package org.eclipse.paho.android.service;

import android.app.Service;
import android.content.Context;
import android.os.Bundle;
import android.os.PowerManager;
import android.os.PowerManager.WakeLock;
import android.util.Log;

import org.eclipse.paho.android.service.MessageStore.StoredMessage;
import org.eclipse.paho.client.mqttv3.DisconnectedBufferOptions;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.eclipse.paho.android.service.LogUtils.LOGD;
import static org.eclipse.paho.android.service.LogUtils.LOGE;
import static org.eclipse.paho.android.service.LogUtils.LOGI;

/**
 * <p>
 * MqttConnection holds a MqttAsyncClient {host,port,clientId} instance to perform
 * MQTT operations to MQTT broker.
 * </p>
 * <p>
 * Most of the major API here is intended to implement the most general forms of
 * the methods in IMqttAsyncClient, with slight adjustments for the Android
 * environment<br>
 * These adjustments usually consist of adding two parameters to each method :-
 * <ul>
 * <li>invocationContext - a string passed from the application to identify the
 * context of the operation (mainly included for support of the javascript API
 * implementation)</li>
 * <li>activityToken - a string passed from the Activity to relate back to a
 * callback method or other context-specific data</li>
 * </ul>
 * </p>
 * <p>
 * Operations are very much asynchronous, so success and failure are notified by
 * packing the relevant data into Intent objects which are broadcast back to the
 * Activity via the MqttService.callbackToActivity() method.
 * </p>
 */
class MqttConnection implements MqttCallbackExtended
{

    // Strings for Intents etc..
    private static final String TAG = "MqttConnection";
    // Error status messages
    private static final String NOT_CONNECTED = "not connected";

    private Context mContext;
    // fields for the connection definition
    private String serverURI;
    private String clientId;
    private MqttClientPersistence persistence;
    private MqttConnectOptions connectOptions;
    // Client handle, used for callbacks...
    private String clientHandle;
    //store connect ActivityToken for reconnect
    private String reconnectActivityToken;
    // our client object - instantiated on connect
    private MqttAsyncClient mMqttAsyncClient;
    private AlarmPingSender alarmPingSender;
    // our (parent) mMqttConnectionManager object
    private MqttTraceHandler mMqttTraceHandler;
    private MessageStore mMessageStore;
    private volatile boolean disconnected;
    private boolean cleanSession;
    // Indicate this connection is connecting or not.
    // This variable uses to avoid reconnect multiple times.
    private volatile boolean isConnecting;
    // Saved sent messages and their corresponding Topics, activityTokens and
    // invocationContexts, so we can handle "deliveryComplete" callbacks
    // from the mqttClient
    private Map<IMqttDeliveryToken, String /* Topic */> savedTopics = new HashMap<>();
    private Map<IMqttDeliveryToken, MqttMessage> savedSentMessages = new HashMap<>();
    private Map<IMqttDeliveryToken, String> savedActivityTokens = new HashMap<>();
    private Map<IMqttDeliveryToken, String> savedInvocationContexts = new HashMap<>();
    private WakeLock wakelock = null;
    private String wakeLockTag = null;
    private DisconnectedBufferOptions bufferOpts = null;
    private Callback mCallback;

    /**
     * Constructor - create an MqttConnection to communicate with MQTT server
     *
     * @param serverURI    the URI of the MQTT server to which we will connect
     * @param clientId     the name by which we will identify ourselves to the MQTT
     *                     server
     * @param persistence  the persistence class to use to store in-flight message. If
     *                     null then the default persistence mechanism is used
     * @param clientHandle the "handle" by which the activity will identify us
     */
    MqttConnection(Context context, MessageStore messageStore, String serverURI, String clientId,
            MqttClientPersistence persistence, String clientHandle, MqttTraceHandler mqttTraceHandler, Callback
            callback)
    {
        this.mContext = context.getApplicationContext();
        this.serverURI = serverURI;
        this.clientId = clientId;
        this.persistence = persistence;
        this.clientHandle = clientHandle;
        this.mMessageStore = messageStore;
        this.mMqttTraceHandler = mqttTraceHandler;
        this.mCallback = callback;
        this.wakeLockTag = this.getClass().getCanonicalName() + " " + clientId + " on host " + serverURI;
    }

    public String getServerURI()
    {
        return serverURI;
    }

    public void setServerURI(String serverURI)
    {
        this.serverURI = serverURI;
    }

    public String getClientId()
    {
        return clientId;
    }

    public void setClientId(String clientId)
    {
        this.clientId = clientId;
    }

    public MqttConnectOptions getConnectOptions()
    {
        return connectOptions;
    }

    public void setConnectOptions(MqttConnectOptions connectOptions)
    {
        this.connectOptions = connectOptions;
    }

    public String getClientHandle()
    {
        return clientHandle;
    }

    public void setClientHandle(String clientHandle)
    {
        this.clientHandle = clientHandle;
    }

    // The major API implementation follows

    /**
     * Connect to the server specified when we were instantiated
     *
     * @param options           timeout, etc
     * @param invocationContext arbitrary data to be passed back to the application
     * @param activityToken     arbitrary identifier to be passed back to the Activity
     */
    public void connect(MqttConnectOptions options, String invocationContext, String activityToken)
    {

        connectOptions = options;
        reconnectActivityToken = activityToken;

        if (options != null)
        {
            cleanSession = options.isCleanSession();
        }

        if (connectOptions.isCleanSession())
        {
            // if it's a clean session,discard old data
            mMessageStore.clearArrivedMessages(clientHandle);
        }

        mMqttTraceHandler.traceDebug(TAG, "Connecting {" + serverURI + "} as {" + clientId + "}");
        final Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN, activityToken);
        resultBundle.putString(MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT, invocationContext);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION, MqttServiceConstants.CONNECT_ACTION);


        try
        {
            if (persistence == null)
            {
                // ask Android where we can put files
                File myDir = mContext.getExternalFilesDir(TAG);

                if (myDir == null)
                {
                    // No external storage, use internal storage instead.
                    myDir = mContext.getDir(TAG, Context.MODE_PRIVATE);

                    if (myDir == null)
                    {
                        //Shouldn't happen.
                        resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE,
                                "Error! No external and internal storage available");
                        resultBundle.putSerializable(MqttServiceConstants.CALLBACK_EXCEPTION,
                                new MqttPersistenceException());
                        notifyCallback(clientHandle, Status.ERROR, resultBundle);
                        return;
                    }
                }

                // use that to setup MQTT client persistence storage
                persistence = new MqttDefaultFilePersistence(myDir.getAbsolutePath());
            }

            IMqttActionListener listener = new MqttConnectionListener(resultBundle)
            {

                @Override
                public void onSuccess(IMqttToken asyncActionToken)
                {
                    doAfterConnectSuccess(resultBundle);
                    mMqttTraceHandler.traceDebug(TAG, "connect success!");
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken,
                        Throwable exception)
                {
                    resultBundle
                            .putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE, exception.getLocalizedMessage());
                    resultBundle.putSerializable(MqttServiceConstants.CALLBACK_EXCEPTION, exception);
                    mMqttTraceHandler
                            .traceError(TAG,
                                    "connect fail, call connect to reconnect.reason:" + exception.getMessage());

                    doAfterConnectFail(resultBundle);

                }
            };

            if (mMqttAsyncClient != null)
            {
                if (isConnecting)
                {
                    mMqttTraceHandler.traceDebug(TAG,
                            "mMqttAsyncClient != null and the client is connecting. Connect return directly.");
                    mMqttTraceHandler.traceDebug(TAG,
                            "Connect return:isConnecting:" + isConnecting + ".disconnected:" + disconnected);
                }
                else if (!disconnected)
                {
                    mMqttTraceHandler
                            .traceDebug(TAG, "mMqttAsyncClient != null and the client is connected and notify!");
                    doAfterConnectSuccess(resultBundle);
                }
                else
                {
                    mMqttTraceHandler.traceDebug(TAG, "mMqttAsyncClient != null and the client is not connected");
                    mMqttTraceHandler.traceDebug(TAG, "Do Real connect!");
                    setConnectingState(true);
                    mMqttAsyncClient.connect(connectOptions, invocationContext, listener);
                }
            }

            // if mMqttAsyncClient is null, then create a new connection
            else
            {
                alarmPingSender = new AlarmPingSender(mContext);
                mMqttAsyncClient = new MqttAsyncClient(serverURI, clientId, persistence, alarmPingSender);
                mMqttAsyncClient.setCallback(this);

                mMqttTraceHandler.traceDebug(TAG, "Do Real connect!");
                setConnectingState(true);
                mMqttAsyncClient.connect(connectOptions, invocationContext, listener);
            }
        }
        catch (Exception e)
        {
            mMqttTraceHandler.traceError(TAG, "Exception occurred attempting to connect: " + e.getMessage());
            setConnectingState(false);
            handleException(resultBundle, e);
        }
    }

    private void doAfterConnectSuccess(final Bundle resultBundle)
    {
        //since the device's cpu can go to sleep, acquire a wakelock and drop it later.
        acquireWakeLock();
        notifyCallback(clientHandle, Status.OK, resultBundle);
        deliverBacklog();
        setConnectingState(false);
        disconnected = false;
        releaseWakeLock();
    }

    @Override
    public void connectComplete(boolean reconnect, String serverURI)
    {
        Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION, MqttServiceConstants.CONNECT_EXTENDED_ACTION);
        resultBundle.putBoolean(MqttServiceConstants.CALLBACK_RECONNECT, reconnect);
        resultBundle.putString(MqttServiceConstants.CALLBACK_SERVER_URI, serverURI);
        notifyCallback(clientHandle, Status.OK, resultBundle);
    }

    private void doAfterConnectFail(final Bundle resultBundle)
    {
        acquireWakeLock();
        disconnected = true;
        setConnectingState(false);
        notifyCallback(clientHandle, Status.ERROR, resultBundle);
        releaseWakeLock();
    }

    private void handleException(final Bundle resultBundle, Exception e)
    {
        resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE,
                e.getLocalizedMessage());

        resultBundle.putSerializable(MqttServiceConstants.CALLBACK_EXCEPTION, e);

        notifyCallback(clientHandle, Status.ERROR, resultBundle);
    }

    /**
     * Attempt to deliver any outstanding messages we've received but which the
     * application hasn't acknowledged. If "cleanSession" was specified, we'll
     * have already purged any such messages from our messageStore.
     */
    private void deliverBacklog()
    {
        Iterator<StoredMessage> backlog = mMessageStore.getAllArrivedMessages(clientHandle);
        while (backlog.hasNext())
        {
            StoredMessage msgArrived = backlog.next();
            Bundle resultBundle = messageToBundle(msgArrived.getMessageId(), msgArrived.getTopic(),
                    msgArrived.getMessage());
            resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION, MqttServiceConstants.MESSAGE_ARRIVED_ACTION);
            notifyCallback(clientHandle, Status.OK, resultBundle);
        }
    }

    /**
     * Create a bundle containing all relevant data pertaining to a message
     *
     * @param messageId the message's identifier in the messageStore, so that a
     *                  callback can be made to remove it once delivered
     * @param topic     the topic on which the message was delivered
     * @param message   the message itself
     * @return the bundle
     */
    private Bundle messageToBundle(String messageId, String topic,
            MqttMessage message)
    {
        Bundle result = new Bundle();
        result.putString(MqttServiceConstants.CALLBACK_MESSAGE_ID, messageId);
        result.putString(MqttServiceConstants.CALLBACK_DESTINATION_NAME, topic);
        result.putParcelable(MqttServiceConstants.CALLBACK_MESSAGE_PARCEL,
                new ParcelableMqttMessage(message));
        return result;
    }

    /**
     * Close connection from the server
     */
    void close()
    {
        mMqttTraceHandler.traceDebug(TAG, "close()");
        try
        {
            if (mMqttAsyncClient != null)
            {
                mMqttAsyncClient.close();
            }
        }
        catch (MqttException e)
        {
            // Pass a new bundle, let handleException stores error messages.
            handleException(new Bundle(), e);
        }
    }

    /**
     * Disconnect from the server
     *
     * @param quiesceTimeout    in milliseconds
     * @param invocationContext arbitrary data to be passed back to the application
     * @param activityToken     arbitrary string to be passed back to the activity
     */
    void disconnect(long quiesceTimeout, String invocationContext,
            String activityToken)
    {
        mMqttTraceHandler.traceDebug(TAG, "disconnect()");
        disconnected = true;
        final Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN, activityToken);
        resultBundle.putString(MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT, invocationContext);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION, MqttServiceConstants.DISCONNECT_ACTION);
        if ((mMqttAsyncClient != null) && (mMqttAsyncClient.isConnected()))
        {
            IMqttActionListener listener = new MqttConnectionListener(
                    resultBundle);
            try
            {
                mMqttAsyncClient.disconnect(quiesceTimeout, invocationContext, listener);
            }
            catch (Exception e)
            {
                handleException(resultBundle, e);
            }
        }
        else
        {
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE, NOT_CONNECTED);
            mMqttTraceHandler.traceError(MqttServiceConstants.DISCONNECT_ACTION, NOT_CONNECTED);
            notifyCallback(clientHandle, Status.ERROR, resultBundle);
        }

        if (connectOptions != null && connectOptions.isCleanSession())
        {
            // assume we'll clear the stored messages at this point
            mMessageStore.clearArrivedMessages(clientHandle);
        }

        releaseWakeLock();
    }

    /**
     * Disconnect from the server
     *
     * @param invocationContext arbitrary data to be passed back to the application
     * @param activityToken     arbitrary string to be passed back to the activity
     */
    void disconnect(String invocationContext, String activityToken)
    {
        mMqttTraceHandler.traceDebug(TAG, "disconnect()");
        disconnected = true;
        final Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN, activityToken);
        resultBundle.putString(MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT, invocationContext);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION, MqttServiceConstants.DISCONNECT_ACTION);
        if ((mMqttAsyncClient != null) && (mMqttAsyncClient.isConnected()))
        {
            IMqttActionListener listener = new MqttConnectionListener(
                    resultBundle);
            try
            {
                mMqttAsyncClient.disconnect(invocationContext, listener);
            }
            catch (Exception e)
            {
                handleException(resultBundle, e);
            }
        }
        else
        {
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE, NOT_CONNECTED);
            mMqttTraceHandler.traceError(MqttServiceConstants.DISCONNECT_ACTION, NOT_CONNECTED);
            notifyCallback(clientHandle, Status.ERROR, resultBundle);
        }

        if (connectOptions != null && connectOptions.isCleanSession())
        {
            // assume we'll clear the stored messages at this point
            mMessageStore.clearArrivedMessages(clientHandle);
        }
        releaseWakeLock();
    }

    /**
     * @return true if we are connected to an MQTT server
     */
    public boolean isConnected()
    {
        return mMqttAsyncClient != null && mMqttAsyncClient.isConnected();
    }

    /**
     * Publish a message on a topic
     *
     * @param topic             the topic on which to publish - represented as a string, not
     *                          an MqttTopic object
     * @param payload           the content of the message to publish
     * @param qos               the quality of mMqttConnectionManager requested
     * @param retained          whether the MQTT server should retain this message
     * @param invocationContext arbitrary data to be passed back to the application
     * @param activityToken     arbitrary string to be passed back to the activity
     * @return token for tracking the operation
     */
    public IMqttDeliveryToken publish(String topic, byte[] payload, int qos, boolean retained, String
            invocationContext, String activityToken)
    {
        final Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION, MqttServiceConstants.SEND_ACTION);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN, activityToken);
        resultBundle.putString(MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT, invocationContext);

        IMqttDeliveryToken sendToken = null;

        if ((mMqttAsyncClient != null) && (mMqttAsyncClient.isConnected()))
        {
            IMqttActionListener listener = new MqttConnectionListener(
                    resultBundle);
            try
            {
                MqttMessage message = new MqttMessage(payload);
                message.setQos(qos);
                message.setRetained(retained);
                sendToken = mMqttAsyncClient.publish(topic, payload, qos, retained,
                        invocationContext, listener);
                storeSendDetails(topic, message, sendToken, invocationContext,
                        activityToken);
            }
            catch (Exception e)
            {
                handleException(resultBundle, e);
            }
        }
        else
        {
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE, NOT_CONNECTED);
            mMqttTraceHandler.traceError(MqttServiceConstants.SEND_ACTION, NOT_CONNECTED);
            notifyCallback(clientHandle, Status.ERROR, resultBundle);
        }

        return sendToken;
    }

    /**
     * Publish a message on a topic
     *
     * @param topic             the topic on which to publish - represented as a string, not
     *                          an MqttTopic object
     * @param message           the message to publish
     * @param invocationContext arbitrary data to be passed back to the application
     * @param activityToken     arbitrary string to be passed back to the activity
     * @return token for tracking the operation
     */
    public IMqttDeliveryToken publish(String topic, MqttMessage message,
            String invocationContext, String activityToken)
    {
        final Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION,
                MqttServiceConstants.SEND_ACTION);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN,
                activityToken);
        resultBundle.putString(
                MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT,
                invocationContext);

        IMqttDeliveryToken sendToken = null;

        if ((mMqttAsyncClient != null) && (mMqttAsyncClient.isConnected()))
        {
            IMqttActionListener listener = new MqttConnectionListener(
                    resultBundle);
            try
            {
                sendToken = mMqttAsyncClient.publish(topic, message, invocationContext,
                        listener);
                storeSendDetails(topic, message, sendToken, invocationContext,
                        activityToken);
            }
            catch (Exception e)
            {
                handleException(resultBundle, e);
            }
        }
        else if ((mMqttAsyncClient != null) && (this.bufferOpts != null) && (this.bufferOpts.isBufferEnabled()))
        {
            // Client is not connected, but buffer is enabled, so sending message
            IMqttActionListener listener = new MqttConnectionListener(
                    resultBundle);
            try
            {
                sendToken = mMqttAsyncClient.publish(topic, message, invocationContext,
                        listener);
                storeSendDetails(topic, message, sendToken, invocationContext,
                        activityToken);
            }
            catch (Exception e)
            {
                handleException(resultBundle, e);
            }
        }
        else
        {
            LOGI(TAG, "Client is not connected, so not sending message");
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE, NOT_CONNECTED);
            mMqttTraceHandler.traceError(MqttServiceConstants.SEND_ACTION, NOT_CONNECTED);
            notifyCallback(clientHandle, Status.ERROR, resultBundle);
        }
        return sendToken;
    }

    /**
     * Subscribe to a topic
     *
     * @param topic             a possibly wildcarded topic name
     * @param qos               requested quality of mMqttConnectionManager for the topic
     * @param invocationContext arbitrary data to be passed back to the application
     * @param activityToken     arbitrary identifier to be passed back to the Activity
     */
    public void subscribe(final String topic, final int qos,
            String invocationContext, String activityToken)
    {
        mMqttTraceHandler.traceDebug(TAG, "subscribe({" + topic + "}," + qos + ",{"
                + invocationContext + "}, {" + activityToken + "}");
        final Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION, MqttServiceConstants.SUBSCRIBE_ACTION);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN, activityToken);
        resultBundle.putString(MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT, invocationContext);

        if ((mMqttAsyncClient != null) && (mMqttAsyncClient.isConnected()))
        {
            IMqttActionListener listener = new MqttConnectionListener(resultBundle);
            try
            {
                mMqttAsyncClient.subscribe(topic, qos, invocationContext, listener);
            }
            catch (Exception e)
            {
                handleException(resultBundle, e);
            }
        }
        else
        {
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE, NOT_CONNECTED);
            mMqttTraceHandler.traceError("subscribe", NOT_CONNECTED);
            notifyCallback(clientHandle, Status.ERROR, resultBundle);
        }
    }

    /**
     * Subscribe to one or more topics
     *
     * @param topic             a list of possibly wildcarded topic names
     * @param qos               requested quality of mMqttConnectionManager for each topic
     * @param invocationContext arbitrary data to be passed back to the application
     * @param activityToken     arbitrary identifier to be passed back to the Activity
     */
    public void subscribe(final String[] topic, final int[] qos,
            String invocationContext, String activityToken)
    {
        mMqttTraceHandler
                .traceDebug(TAG, "subscribe({" + Arrays.toString(topic) + "}," + Arrays.toString(qos) + ",{"
                        + invocationContext + "}, {" + activityToken + "}");
        final Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION, MqttServiceConstants.SUBSCRIBE_ACTION);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN, activityToken);
        resultBundle.putString(MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT, invocationContext);

        if ((mMqttAsyncClient != null) && (mMqttAsyncClient.isConnected()))
        {
            IMqttActionListener listener = new MqttConnectionListener(
                    resultBundle);
            try
            {
                mMqttAsyncClient.subscribe(topic, qos, invocationContext, listener);
            }
            catch (Exception e)
            {
                handleException(resultBundle, e);
            }
        }
        else
        {
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE, NOT_CONNECTED);
            mMqttTraceHandler.traceError("subscribe", NOT_CONNECTED);
            notifyCallback(clientHandle, Status.ERROR, resultBundle);
        }
    }

    public void subscribe(String[] topicFilters, int[] qos, String invocationContext, String activityToken,
            IMqttMessageListener[] messageListeners)
    {
        mMqttTraceHandler
                .traceDebug(TAG, "subscribe({" + Arrays.toString(topicFilters) + "}," + Arrays.toString(qos) + ",{"
                        + invocationContext + "}, {" + activityToken + "}");
        final Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION, MqttServiceConstants.SUBSCRIBE_ACTION);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN, activityToken);
        resultBundle.putString(MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT, invocationContext);
        if ((mMqttAsyncClient != null) && (mMqttAsyncClient.isConnected()))
        {
            IMqttActionListener listener = new MqttConnectionListener(resultBundle);
            try
            {

                mMqttAsyncClient.subscribe(topicFilters, qos, messageListeners);
            }
            catch (Exception e)
            {
                handleException(resultBundle, e);
            }
        }
        else
        {
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE, NOT_CONNECTED);
            mMqttTraceHandler.traceError("subscribe", NOT_CONNECTED);
            notifyCallback(clientHandle, Status.ERROR, resultBundle);
        }
    }

    /**
     * Unsubscribe from a topic
     *
     * @param topic             a possibly wildcarded topic name
     * @param invocationContext arbitrary data to be passed back to the application
     * @param activityToken     arbitrary identifier to be passed back to the Activity
     */
    void unsubscribe(final String topic, String invocationContext,
            String activityToken)
    {
        mMqttTraceHandler
                .traceDebug(TAG, "unsubscribe({" + topic + "},{" + invocationContext + "}, {" + activityToken + "})");
        final Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION, MqttServiceConstants.UNSUBSCRIBE_ACTION);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN, activityToken);
        resultBundle.putString(MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT, invocationContext);
        if ((mMqttAsyncClient != null) && (mMqttAsyncClient.isConnected()))
        {
            IMqttActionListener listener = new MqttConnectionListener(
                    resultBundle);
            try
            {
                mMqttAsyncClient.unsubscribe(topic, invocationContext, listener);
            }
            catch (Exception e)
            {
                handleException(resultBundle, e);
            }
        }
        else
        {
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE, NOT_CONNECTED);

            mMqttTraceHandler.traceError("subscribe", NOT_CONNECTED);
            notifyCallback(clientHandle, Status.ERROR, resultBundle);
        }
    }

    /**
     * Unsubscribe from one or more topics
     *
     * @param topic             a list of possibly wildcarded topic names
     * @param invocationContext arbitrary data to be passed back to the application
     * @param activityToken     arbitrary identifier to be passed back to the Activity
     */
    void unsubscribe(final String[] topic, String invocationContext,
            String activityToken)
    {
        mMqttTraceHandler.traceDebug(TAG,
                "unsubscribe({" + Arrays.toString(topic) + "},{" + invocationContext + "}, {" + activityToken + "})");
        final Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION, MqttServiceConstants.UNSUBSCRIBE_ACTION);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN, activityToken);
        resultBundle.putString(MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT, invocationContext);
        if ((mMqttAsyncClient != null) && (mMqttAsyncClient.isConnected()))
        {
            IMqttActionListener listener = new MqttConnectionListener(
                    resultBundle);
            try
            {
                mMqttAsyncClient.unsubscribe(topic, invocationContext, listener);
            }
            catch (Exception e)
            {
                handleException(resultBundle, e);
            }
        }
        else
        {
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE,
                    NOT_CONNECTED);

            mMqttTraceHandler.traceError("subscribe", NOT_CONNECTED);
            notifyCallback(clientHandle, Status.ERROR, resultBundle);
        }
    }

    /**
     * Get tokens for all outstanding deliveries for a client
     *
     * @return an array (possibly empty) of tokens
     */
    public IMqttDeliveryToken[] getPendingDeliveryTokens()
    {
        return mMqttAsyncClient.getPendingDeliveryTokens();
    }

    // Implement MqttCallback

    /**
     * Callback for connectionLost
     *
     * @param throwable the exeception causing the break in communications
     */
    @Override
    public void connectionLost(Throwable throwable)
    {
        mMqttTraceHandler.traceDebug(TAG, "connectionLost(" + throwable.getMessage() + ")");
        disconnected = true;
        try
        {
            if (!this.connectOptions.isAutomaticReconnect())
            {
                mMqttAsyncClient.disconnect(null, new IMqttActionListener()
                {

                    @Override
                    public void onSuccess(IMqttToken asyncActionToken)
                    {
                        // No action
                        LOGD(TAG, "Disconnected successfully");
                    }

                    @Override
                    public void onFailure(IMqttToken asyncActionToken, Throwable exception)
                    {
                        // No action
                        LOGD(TAG, "Disconnection error: " + (exception == null ? "null" : exception.getMessage()));
                    }
                });
            }
            else
            {
                // Using the new Automatic reconnect functionality.
                // We can't force a disconnection, but we can speed one up
                alarmPingSender.schedule(100);

            }
        }
        catch (Exception e)
        {
            // ignore it - we've done our best
        }

        Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION, MqttServiceConstants.ON_CONNECTION_LOST_ACTION);
        if (throwable != null)
        {
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE, throwable.getMessage());
            if (throwable instanceof MqttException)
            {
                resultBundle.putSerializable(MqttServiceConstants.CALLBACK_EXCEPTION, throwable);
            }
            resultBundle.putString(MqttServiceConstants.CALLBACK_EXCEPTION_STACK, Log.getStackTraceString(throwable));
        }
        notifyCallback(clientHandle, Status.OK, resultBundle);
        // client has lost connection no need for wake lock
        releaseWakeLock();
    }

    /**
     * Callback to indicate a message has been delivered (the exact meaning of
     * "has been delivered" is dependent on the QOS value)
     *
     * @param messageToken the messge token provided when the message was originally sent
     */
    @Override
    public void deliveryComplete(IMqttDeliveryToken messageToken)
    {

        mMqttTraceHandler.traceDebug(TAG, "deliveryComplete(" + messageToken + ")");

        MqttMessage message = savedSentMessages.remove(messageToken);
        if (message != null)
        { // If I don't know about the message, it's
            // irrelevant
            String topic = savedTopics.remove(messageToken);
            String activityToken = savedActivityTokens.remove(messageToken);
            String invocationContext = savedInvocationContexts.remove(messageToken);

            Bundle resultBundle = messageToBundle(null, topic, message);
            if (activityToken != null)
            {
                resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION, MqttServiceConstants.SEND_ACTION);
                resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN, activityToken);
                resultBundle.putString(MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT, invocationContext);

                notifyCallback(clientHandle, Status.OK, resultBundle);
            }
            resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION, MqttServiceConstants.MESSAGE_DELIVERED_ACTION);
            notifyCallback(clientHandle, Status.OK, resultBundle);
        }

        // this notification will have kept the connection alive but send the previously sechudled ping anyway
    }

    /**
     * Callback when a message is received
     *
     * @param topic   the topic on which the message was received
     * @param message the message itself
     */
    @Override
    public void messageArrived(String topic, MqttMessage message)
            throws Exception
    {

        mMqttTraceHandler.traceDebug(TAG, "messageArrived(" + topic + ",{" + message.toString() + "})");

        String messageId = mMessageStore.storeArrived(clientHandle, topic, message);

        Bundle resultBundle = messageToBundle(messageId, topic, message);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION, MqttServiceConstants.MESSAGE_ARRIVED_ACTION);
        resultBundle.putString(MqttServiceConstants.CALLBACK_MESSAGE_ID, messageId);
        notifyCallback(clientHandle, Status.OK, resultBundle);

    }


    /**
     * Store details of sent messages so we can handle "deliveryComplete"
     * callbacks from the mqttClient
     *
     * @param topic
     * @param msg
     * @param messageToken
     * @param invocationContext
     * @param activityToken
     */
    private void storeSendDetails(final String topic, final MqttMessage msg, final IMqttDeliveryToken messageToken,
            final String invocationContext, final String activityToken)
    {
        savedTopics.put(messageToken, topic);
        savedSentMessages.put(messageToken, msg);
        savedActivityTokens.put(messageToken, activityToken);
        savedInvocationContexts.put(messageToken, invocationContext);
    }

    /**
     * Acquires a partial wake lock for this client
     */
    private void acquireWakeLock()
    {
        if (wakelock == null)
        {
            PowerManager pm = (PowerManager) mContext.getSystemService(Service.POWER_SERVICE);
            //noinspection ConstantConditions
            wakelock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, wakeLockTag);
        }
        wakelock.acquire();

    }

    /**
     * Releases the currently held wake lock for this client
     */
    private void releaseWakeLock()
    {
        if (wakelock != null && wakelock.isHeld())
        {
            wakelock.release();
        }
    }

    /**
     * Receive notification that we are offline<br>
     * if cleanSession is true, we need to regard this as a disconnection
     */
    void offline()
    {

        if (!disconnected && !cleanSession)
        {
            Exception e = new Exception("Android offline");
            connectionLost(e);
        }
    }

    /**
     * Reconnect<br>
     * Only appropriate if cleanSession is false and we were connected.
     * Declare as synchronized to avoid multiple calls to this method to send connect
     * multiple times
     */
    synchronized void reconnect()
    {

        if (mMqttAsyncClient == null)
        {
            mMqttTraceHandler.traceError(TAG, "Reconnect mMqttAsyncClient = null. Will not do reconnect");
            return;
        }

        if (isConnecting)
        {
            mMqttTraceHandler.traceDebug(TAG, "The client is connecting. Reconnect return directly.");
            return;
        }

        if (!Utils.isOnline(mContext))
        {
            mMqttTraceHandler.traceDebug(TAG,
                    "The network is not reachable. Will not do reconnect");
            return;
        }

        if (connectOptions.isAutomaticReconnect())
        {
            //The Automatic reconnect functionality is enabled here
            LOGI(TAG, "Requesting Automatic reconnect using New Java AC");
            final Bundle resultBundle = new Bundle();
            resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN, reconnectActivityToken);
            resultBundle.putString(MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT, null);
            resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION, MqttServiceConstants.CONNECT_ACTION);
            try
            {
                mMqttAsyncClient.reconnect();
            }
            catch (MqttException ex)
            {
                LOGE(TAG, "Exception occurred attempting to reconnect: " + ex.getMessage());
                setConnectingState(false);
                handleException(resultBundle, ex);
            }
        }
        else if (disconnected && !cleanSession)
        {
            // use the activityToke the same with action connect
            mMqttTraceHandler.traceDebug(TAG, "Do Real Reconnect!");
            final Bundle resultBundle = new Bundle();
            resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN, reconnectActivityToken);
            resultBundle.putString(MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT, null);
            resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION, MqttServiceConstants.CONNECT_ACTION);

            try
            {

                IMqttActionListener listener = new MqttConnectionListener(resultBundle)
                {
                    @Override
                    public void onSuccess(IMqttToken asyncActionToken)
                    {
                        // since the device's cpu can go to sleep, acquire a
                        // wakelock and drop it later.
                        mMqttTraceHandler.traceDebug(TAG, "Reconnect Success!");
                        mMqttTraceHandler.traceDebug(TAG, "DeliverBacklog when reconnect.");
                        doAfterConnectSuccess(resultBundle);
                    }

                    @Override
                    public void onFailure(IMqttToken asyncActionToken, Throwable exception)
                    {
                        resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE,
                                exception.getLocalizedMessage());
                        resultBundle.putSerializable(MqttServiceConstants.CALLBACK_EXCEPTION, exception);
                        notifyCallback(clientHandle, Status.ERROR, resultBundle);

                        doAfterConnectFail(resultBundle);

                    }
                };

                mMqttAsyncClient.connect(connectOptions, null, listener);
                setConnectingState(true);
            }
            catch (MqttException e)
            {
                mMqttTraceHandler.traceError(TAG, "Cannot reconnect to remote server." + e.getMessage());
                setConnectingState(false);
                handleException(resultBundle, e);
            }
            catch (Exception e)
            {
                /*  TODO: Added Due to: https://github.com/eclipse/paho.mqtt.android/issues/101
                    For some reason in a small number of cases, mMqttAsyncClient is null here and so
				    a NullPointer Exception is thrown. This is a workaround to pass the exception
				    up to the application. mMqttAsyncClient should not be null so more investigation is
				    required.
				*/
                mMqttTraceHandler.traceError(TAG, "Cannot reconnect to remote server." + e.getMessage());
                setConnectingState(false);
                MqttException newEx = new MqttException(MqttException.REASON_CODE_UNEXPECTED_ERROR, e.getCause());
                handleException(resultBundle, newEx);
            }
        }
    }

    /**
     * @param isConnecting
     */
    private synchronized void setConnectingState(boolean isConnecting)
    {
        this.isConnecting = isConnecting;
    }

    /**
     * Sets the DisconnectedBufferOptions for this client
     *
     * @param bufferOpts
     */
    public void setBufferOpts(DisconnectedBufferOptions bufferOpts)
    {
        this.bufferOpts = bufferOpts;
        mMqttAsyncClient.setBufferOpts(bufferOpts);
    }

    public int getBufferedMessageCount()
    {
        return mMqttAsyncClient.getBufferedMessageCount();
    }

    public MqttMessage getBufferedMessage(int bufferIndex)
    {
        return mMqttAsyncClient.getBufferedMessage(bufferIndex);
    }

    public void deleteBufferedMessage(int bufferIndex)
    {
        mMqttAsyncClient.deleteBufferedMessage(bufferIndex);
    }

    private void notifyCallback(String clientHandle, Status status, Bundle dataBundle)
    {
        if (mCallback != null)
            mCallback.onEvent(clientHandle, status, dataBundle);
    }

    public interface Callback
    {
        void onEvent(String clientHandle, Status status, Bundle dataBundle);
    }

    /**
     * General-purpose IMqttActionListener for the Client context
     * <p>
     * Simply handles the basic success/failure cases for operations which don't
     * return results
     */
    private class MqttConnectionListener implements IMqttActionListener
    {

        private final Bundle resultBundle;

        private MqttConnectionListener(Bundle resultBundle)
        {
            this.resultBundle = resultBundle;
        }

        @Override
        public void onSuccess(IMqttToken asyncActionToken)
        {
            notifyCallback(clientHandle, Status.OK, resultBundle);
        }

        @Override
        public void onFailure(IMqttToken asyncActionToken, Throwable exception)
        {
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE, exception.getLocalizedMessage());
            resultBundle.putSerializable(MqttServiceConstants.CALLBACK_EXCEPTION, exception);
            notifyCallback(clientHandle, Status.ERROR, resultBundle);
        }
    }
}
