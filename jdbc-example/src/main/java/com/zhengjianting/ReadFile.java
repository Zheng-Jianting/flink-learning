package com.zhengjianting;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ReadFile {
    public static String file2String(String file) {
        FileInputStream fis = null;
        BufferedInputStream bis = null;

        // 定义一个输出流, 相当StringBuffer(), 会根据读取数据的大小, 调整 byte 数组长度
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
            fis = new FileInputStream(file);
            bis = new BufferedInputStream(fis);

            int len;
            byte[] buf = new byte[1024];
            while ((len = bis.read(buf)) != -1) {
                // 这里直接用字符串 StringBuffer 的 append 方法也可以接收
                baos.write(buf, 0, len);
            }

            System.out.println(baos.toString(String.valueOf(StandardCharsets.UTF_8)));
            return baos.toString(String.valueOf(StandardCharsets.UTF_8));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                baos.close();
                bis.close();
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        return null;
    }
}