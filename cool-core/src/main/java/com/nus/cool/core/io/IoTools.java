/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.nus.cool.core.io;


import com.nus.cool.core.io.compression.Compressor;

import java.io.*;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.util.LogVerificationInputStream;

public class IoTools {

	/**
	 * Delete files in a folder. The parent folder may be optionally deleted as
	 * well.
	 * 
	 * @param parent
	 *            the folder to be deleted
	 * @param rmparent
	 *            true to delete parent, false to keep parent
	 * @throws IOException
	 */
	public static void deleteFully(File parent, boolean rmparent)
			throws IOException {
		if (parent.isDirectory()) {
			File[] files = parent.listFiles();
			for (File f : files) {
				if (f.isDirectory())
					deleteFully(f, true);
				else
					f.delete();
			}
		}

		if (rmparent)
			parent.delete();
	}

	/**
	 * Make an empty directory. If the given directory exists, all contents will
	 * be deleted. Otherwise, the given directory will be created.
	 * 
	 * @param dir
	 *            the directory to be empty
	 * @throws IOException
	 */

	public static void mkempty(File dir) throws IOException {
		if (dir.exists()) {
			deleteFully(dir, false);
			return;
		}
		dir.mkdirs();
	}

	public static void closeQuietly(Closeable... closes) {
		for (Closeable close : closes) {
			try {
				close.close();
			} catch (IOException e) {
			}
		}
	}

	public static void copyJeFile(final Environment env, final String fileName,
			OutputStream os, final int bufSize) throws IOException,
			DatabaseException {
		final File srcDir = env.getHome();
		final File srcFile = new File(srcDir, fileName);
		final FileInputStream fis = new FileInputStream(srcFile);
		final LogVerificationInputStream vis = new LogVerificationInputStream(
				env, fis, fileName);
		final byte[] buf = new byte[bufSize];

		try {
			while (true) {
				final int len = vis.read(buf);
				if (len < 0) {
					break;
				}
				os.write(buf, 0, len);
			}
		} finally {
			IoTools.closeQuietly(os, vis);
		}
	}

	public static void copyJeFiles(final Environment env,
			final String[] fileNames, final File destDir, final int bufSize)
			throws IOException, DatabaseException {

		for (final String fileName : fileNames) {
			final File destFile = new File(destDir, fileName);
			copyJeFile(env, fileName, new FileOutputStream(destFile), bufSize);
		}
	}
	
	public static DataOutputStream newFileDataOutputStream(File file) throws IOException {
		return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
	}
	
	public static void writeTo(Compressor comp, DataOutput out, int[] vec,
			int off, int len) throws IOException {
		int maxLen = comp.maxCompressedLength();
		byte[] compressed = new byte[maxLen];
		int compressLen = comp.compress(vec, off, len, compressed, 0, maxLen);
		out.write(compressed, 0, compressLen);
	}

	public static void writeTo(Compressor comp, DataOutput out, byte[] vec,
							   int off, int len) throws IOException {
		int maxLen = comp.maxCompressedLength();
		byte[] compressed = new byte[maxLen];
		int compressLen = comp.compress(vec, off, len, compressed, 0, maxLen);
		out.write(compressed, 0, compressLen);
	}

}
