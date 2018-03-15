package org.eclipse.paho.android.service;

import android.text.TextUtils;
import android.view.View;

import java.lang.reflect.Field;

import timber.log.Timber;

 final class LogUtils
{

    private static final String APP_PREFIX = "Paho_Mqtt_";

    public static String makeLogTag(Class<?> cls)
    {

        return APP_PREFIX + cls.getSimpleName();
    }

    public static void LOGD(String message, Object... args)
    {
        Timber.d(message, args);
    }

    public static void LOGD(String tag, String message, Object... args)
    {
        Timber.tag(tag).d(message, args);
    }


    public static void LOGI(String message, Object... args)
    {
        Timber.i(message, args);
    }

    public static void LOGI(String tag, String message, Object... args)
    {
        Timber.tag(tag).i(message, args);
    }

    public static void LOGW(String message, Object... args)
    {
        Timber.w(message, args);
    }

    public static void LOGW(String tag, String message, Object... args)
    {
        Timber.tag(tag).w(message, args);
    }

    public static void LOGE(String message, Object... args)
    {
        Timber.e(message, args);
    }

    public static void LOGE(String tag, String message, Object... args)
    {
        Timber.tag(tag).e(message, args);
    }

    public static void logMeasureSpec(String tag, int widthMeasureSpec, int heightMeasureSpec)
    {

        int widthSize = View.MeasureSpec.getSize(widthMeasureSpec);
        String widthMode;

        switch (View.MeasureSpec.getMode(widthMeasureSpec))
        {
            case View.MeasureSpec.EXACTLY:
                widthMode = "EXACTLY";
                break;
            case View.MeasureSpec.AT_MOST:
                widthMode = "AT_MOST";
                break;
            default:
                widthMode = "UNSPECIFIED";
        }

        int heightSize = View.MeasureSpec.getSize(heightMeasureSpec);
        String heightMode;

        switch (View.MeasureSpec.getMode(heightMeasureSpec))
        {
            case View.MeasureSpec.EXACTLY:
                heightMode = "EXACTLY";
                break;
            case View.MeasureSpec.AT_MOST:
                heightMode = "AT_MOST";
                break;
            default:
                heightMode = "UNSPECIFIED";
        }

        LOGD(tag, "Width: (%d, %d), Height: ( %d, %d)", widthMode, widthSize, heightMode, heightSize);
    }


    public static String getObjectLogString(Object obj)
    {

        KeyValueLogBuilder keyValueLogBuilder = newKeyValueBuilder();
        try
        {
            for (Field field : obj.getClass().getDeclaredFields())
            {
                field.setAccessible(true);
                String name = field.getName();
                Object value = null;
                value = field.get(obj);
                keyValueLogBuilder.append(name, value == null ? null : value.toString());
            }
        }
        catch (IllegalAccessException e)
        {
            throw new AssertionError();
        }

        return keyValueLogBuilder.build();

    }

    public static KeyValueLogBuilder newKeyValueBuilder()
    {
        return new KeyValueLogBuilder();
    }


    public static class KeyValueLogBuilder
    {

        private StringBuilder mMessage = new StringBuilder();

        private KeyValueLogBuilder()
        {
            // Use static factory method to newSingle instance of this
        }

        public String build()
        {
            return mMessage.toString();
        }

        public KeyValueLogBuilder append(String key, String value)
        {
            if (mMessage.length() > 0)
                mMessage.append(", ");

            mMessage.append(key).append(": ");
            if (!TextUtils.isEmpty(value)) mMessage.append(value);

            return this;
        }

        public KeyValueLogBuilder appendBoolean(String key, boolean value)
        {

            return append(key, getString(value));
        }

        public KeyValueLogBuilder appendInt(String key, int value)
        {

            return append(key, getString(value));
        }

        public KeyValueLogBuilder appendLong(String key, long value)
        {

            return append(key, getString(value));
        }

        public KeyValueLogBuilder appendFloat(String key, float value)
        {

            return append(key, getString(value));
        }

        public KeyValueLogBuilder appendDouble(String key, double value)
        {

            return append(key, getString(value));
        }

        private String getString(Object primitive)
        {
            return String.valueOf(primitive);
        }
    }

}
