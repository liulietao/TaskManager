/**
 * 
 */
package com.youku.cloud.taskmanager.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author liulietao
 *
 */
public class GzipUtil {

	/**
	 * gzip and ungzip class
	 */
	public GzipUtil() {
	}
	
	public static byte[] gzip(byte[] data) throws Exception {
		System.out.println("gzip, before len : " + data.length);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		GZIPOutputStream gzip = new GZIPOutputStream(bos);
		gzip.write(data);
		gzip.finish();
		gzip.close();
		
		byte[] b = bos.toByteArray();
		bos.close();
		System.out.println("gzip, after len : " + b.length);
		return b;
	}

	public static byte[] ungzip(byte[] data) throws Exception {
//		System.out.println("ungzip, before len : " + data.length);
		ByteArrayInputStream bis = new ByteArrayInputStream(data);
		GZIPInputStream gzip = new GZIPInputStream(bis);
		byte[] buf = new byte[1024];
		int num = -1;
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		while((num = gzip.read(buf, 0, buf.length)) != -1) {
			baos.write(buf, 0, num);
		}
		gzip.close();
		bis.close();
		
		byte[] b = baos.toByteArray();
		baos.flush();
		baos.close();
//		System.out.println("ungzip, after len : " + b.length);
		return b;
	}
}
