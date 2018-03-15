package org.eclipse.paho.android.service;

import android.content.Context;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;

import static android.content.Context.CONNECTIVITY_SERVICE;

/**
 * @author Gaurav Dingolia
 */

class Utils
{
    private Utils()
    {
    }

    /**
     * @return whether the android service can be regarded as online
     */
    public static boolean isOnline(Context context)
    {
        ConnectivityManager cm = (ConnectivityManager) context.getSystemService(CONNECTIVITY_SERVICE);
        @SuppressWarnings("ConstantConditions")
        NetworkInfo networkInfo = cm.getActiveNetworkInfo();
        return networkInfo != null && networkInfo.isAvailable() && networkInfo.isConnected();
    }
}
