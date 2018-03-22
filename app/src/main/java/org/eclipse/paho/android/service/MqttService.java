package org.eclipse.paho.android.service;

import android.annotation.SuppressLint;
import android.app.Service;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.net.ConnectivityManager;
import android.os.Binder;
import android.os.IBinder;
import android.os.PowerManager;

import org.eclipse.paho.client.mqttv3.MqttClientPersistence;

@SuppressLint("Registered")
public class MqttService extends Service
{
    // An intent receiver to deal with changes in network connectivity
    private NetworkConnectionIntentReceiver mNetworkConnectionReceiver;
    private MqttConnectionManager mMqttConnectionManager;
    private MqttServiceBinder mMqttServiceBinder;

    @Override
    public void onCreate()
    {
        super.onCreate();
        // create a binder that will let the Activity UI send
        // commands to the Service
        mMqttConnectionManager = new MqttConnectionManager(getApplicationContext());
        mMqttServiceBinder = new MqttServiceBinder(mMqttConnectionManager);
        mNetworkConnectionReceiver = new NetworkConnectionIntentReceiver(mMqttConnectionManager);
        registerReceiver(mNetworkConnectionReceiver, new IntentFilter(ConnectivityManager.CONNECTIVITY_ACTION));
    }


    /**
     * @see android.app.Service#onBind(Intent)
     */
    @Override
    public IBinder onBind(Intent intent)
    {
        // What we pass back to the Activity on binding -
        // a reference to ourself, and the activityToken
        // we were given when started
        String activityToken = intent.getStringExtra(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN);

        mMqttServiceBinder.setActivityToken(activityToken);
        return mMqttServiceBinder;
    }

    @Override
    public void onDestroy()
    {
        super.onDestroy();
        mMqttConnectionManager.disconnectAll();
        mMqttConnectionManager.finish();

        unregisterReceiver(mNetworkConnectionReceiver);
    }

    /*
    * Called in response to a change in network connection - after losing a
    * connection to the server, this allows us to wait until we have a usable
    * data connection again
    */
    private static class NetworkConnectionIntentReceiver extends BroadcastReceiver
    {
        private static final String TAG = NetworkConnectionIntentReceiver.class.getSimpleName();
        private MqttConnectionManager mMqttConnectionManager;

        public NetworkConnectionIntentReceiver(MqttConnectionManager mMqttConnectionManager)
        {
            this.mMqttConnectionManager = mMqttConnectionManager;
        }

        @Override
        @SuppressLint("Wakelock")
        public void onReceive(Context context, Intent intent)
        {
            mMqttConnectionManager.traceDebug(TAG, "Internal network status receive.");
            // we protect against the phone switching off
            // by requesting a wake lock - we request the minimum possible wake
            // lock - just enough to keep the CPU running until we've finished
            PowerManager pm = (PowerManager) context.getSystemService(POWER_SERVICE);
            @SuppressWarnings("ConstantConditions")
            PowerManager.WakeLock wl = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, MqttService.class.getName());
            wl.acquire(Constants.DEFAULT_WAKELOCK_TIMEOUT_MS);
            mMqttConnectionManager.traceDebug(TAG, "Reconnect for Network recovery.");
            if (Utils.isOnline(context))
            {
                mMqttConnectionManager.traceDebug(TAG, "Online,reconnect.");
                // we have an internet connection - have another try at
                // connecting
                mMqttConnectionManager.reconnect();
            }
            else
            {
                mMqttConnectionManager.notifyClientsOffline();
            }

            wl.release();
        }
    }

    public static class MqttServiceBinder extends Binder
    {

        private String activityToken;
        private MqttConnectionManager mMqttConnectionManager;

        MqttServiceBinder(MqttConnectionManager connectionManager)
        {
            mMqttConnectionManager = connectionManager;
        }

        public MqttConnectionHandler initConnection(String serverURI, String clientId, String contextId,
                MqttClientPersistence persistence)
        {
            return mMqttConnectionManager.initConnection(serverURI, clientId, contextId, persistence);
        }

        /**
         * @return the activityToken provided when the Service was started
         */
        public String getActivityToken()
        {
            return activityToken;
        }

        void setActivityToken(String activityToken)
        {
            this.activityToken = activityToken;
        }

    }


}
