package com.strumcode.endb.backend.utils;

public class Panic {
    public static void panic(Exception err) {
        err.printStackTrace();
        // 强制停机
        System.exit(1);
    }
}
