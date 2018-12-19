package com.examples.hops.spark.common;

import scala.util.control.NonFatal;

public class Utils {

    public static void closeAndSuppressed(Throwable exception, AutoCloseable resource) {

            try {
                resource.close();
            } catch (Exception e) {

                if (NonFatal.apply(e) && exception != null) {
                    exception.addSuppressed(e);
                } else e.printStackTrace();
            }
    }
}
