/*******************************************************************************
 * Copyright (c) 2014 IBM Corp.
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

import android.annotation.SuppressLint;
import android.app.AlarmManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.os.Build;
import android.os.PowerManager;
import android.os.PowerManager.WakeLock;
import android.util.Log;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttPingSender;
import org.eclipse.paho.client.mqttv3.internal.ClientComms;

import static org.eclipse.paho.android.service.LogUtils.LOGD;

/**
 * Default ping sender implementation on Android. It is based on AlarmManager.
 * <p>
 * <p>This class implements the {@link MqttPingSender} interface
 * allowing applications to send ping packet to server every keep alive interval.
 * </p>
 *
 * @see MqttPingSender
 */
class AlarmPingSender implements MqttPingSender
{
    private static final String TAG = AlarmPingSender.class.getSimpleName();

    private ClientComms clientComms;
    private Context mApplicationContext;
    private BroadcastReceiver mReceiver;
    private PendingIntent pendingIntent;
    private volatile boolean hasStarted = false;
    private AlarmManager mAlarmManager;

    AlarmPingSender(Context context)
    {
        mApplicationContext = context.getApplicationContext();
        mAlarmManager = (AlarmManager) context.getSystemService(Context.ALARM_SERVICE);
    }

    @Override
    public void init(ClientComms comms)
    {
        this.clientComms = comms;
        this.mReceiver = new AlarmReceiver();
    }

    @Override
    public void start()
    {
        String action = MqttServiceConstants.PING_SENDER + clientComms.getClient().getClientId();

        Context context = mApplicationContext;
        LOGD(TAG, "Register AlarmReceiver to MqttService" + action);
        context.registerReceiver(mReceiver, new IntentFilter(action));

        pendingIntent = PendingIntent.getBroadcast(context, 0, new Intent(action), PendingIntent.FLAG_UPDATE_CURRENT);

        schedule(clientComms.getKeepAlive());
        hasStarted = true;
    }

    @Override
    public void stop()
    {

        LOGD(TAG, "Unregister AlarmReceiver to MqttService" + clientComms.getClient().getClientId());
        if (hasStarted)
        {
            if (pendingIntent != null)
                mAlarmManager.cancel(pendingIntent);

            hasStarted = false;
            try
            {
                mApplicationContext.unregisterReceiver(mReceiver);
            }
            catch (IllegalArgumentException e)
            {
                //Ignore unregister errors.
            }
        }
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    public void schedule(long delayInMilliseconds)
    {
        long nextAlarmInMilliseconds = System.currentTimeMillis() + delayInMilliseconds;
        LOGD(TAG, "Schedule next alarm at " + nextAlarmInMilliseconds);

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M)
        {
            // In SDK 23 and above, dosing will prevent setExact, setExactAndAllowWhileIdle will force
            // the device to run this task whilst dosing.
            LOGD(TAG, "Alarm schedule using setExactAndAllowWhileIdle, next: " + delayInMilliseconds);
            mAlarmManager.setExactAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, nextAlarmInMilliseconds, pendingIntent);
        }
        else if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.KITKAT)
        {
            LOGD(TAG, "Alarm schedule using setExact, delay: " + delayInMilliseconds);
            mAlarmManager.setExact(AlarmManager.RTC_WAKEUP, nextAlarmInMilliseconds, pendingIntent);
        }
        else
        {
            mAlarmManager.set(AlarmManager.RTC_WAKEUP, nextAlarmInMilliseconds, pendingIntent);
        }
    }

    /*
     * This class sends PingReq packet to MQTT broker
     */
    class AlarmReceiver extends BroadcastReceiver
    {
        private final String wakeLockTag = MqttServiceConstants.PING_WAKELOCK + AlarmPingSender.this.clientComms
                .getClient().getClientId();
        private WakeLock wakelock;

        @Override
        @SuppressLint("Wakelock")
        public void onReceive(Context context, Intent intent)
        {
            // According to the docs, "Alarm Manager holds a CPU wake lock as
            // long as the alarm receiver's onReceive() method is executing.
            // This guarantees that the phone will not sleep until you have
            // finished handling the broadcast.", but this class still get
            // a wake lock to wait for ping finished.

            LOGD(TAG, "Sending Ping at:" + System.currentTimeMillis());

            PowerManager pm = (PowerManager) mApplicationContext.getSystemService(Service.POWER_SERVICE);
            //noinspection ConstantConditions
            wakelock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, wakeLockTag);
            wakelock.acquire(Constants.DEFAULT_WAKELOCK_TIMEOUT_MS);

            // Assign new callback to token to execute code after PingResq
            // arrives. Get another wakelock even receiver already has one,
            // release it until ping response returns.
            IMqttToken token = clientComms.checkForActivity(new IMqttActionListener()
            {

                @Override
                public void onSuccess(IMqttToken asyncActionToken)
                {
                    LOGD(TAG, "Success. Release lock(" + wakeLockTag + "):" + System.currentTimeMillis());
                    if (wakelock.isHeld()) wakelock.release();
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken,
                        Throwable exception)
                {
                    LOGD(TAG, "Failure. Release lock(" + wakeLockTag + "):" + System.currentTimeMillis());
                    if (wakelock.isHeld()) wakelock.release();
                }
            });


            if (token == null && wakelock.isHeld()) wakelock.release();
        }
    }
}
