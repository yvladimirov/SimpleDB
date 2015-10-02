package org.simpledb.utils;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Created by yvladimirov on 9/18/15.
 */
public class UnsafeUtils {

    public static Unsafe unsafe = null;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
