package com.eiconnector.ExcelImportConnector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ExcelUtil {

	private static final Logger logger = LoggerFactory.getLogger(ExcelImportConnectorApplication.class);

	private ExcelUtil() {
		super();
	}

	public static void moveFile(String sourceFileName, String destionationFileName) {
		try {
			logger.info("Reading..." + sourceFileName);
			logger.info("Reading..." + destionationFileName);
			File sourceFile = new File(sourceFileName);
			File destinationFile = new File(destionationFileName);

			InputStream in = new FileInputStream(sourceFile);
			logger.info("sourceFile"+sourceFile);
			OutputStream out = new FileOutputStream(destinationFile);
			String fname = sourceFile.getName();
			logger.info("filename::" + fname);

			byte[] buffer = new byte[1024];
			int length;
			while ((length = in.read(buffer)) > 0) {
				out.write(buffer, 0, length);
			}
			in.close();
			out.close();
			logger.info("Copied: " + sourceFileName);
			sourceFile.delete();
			logger.info("File is deleted : " + sourceFile.getAbsolutePath());

		} catch (Exception ex) {

			// LOG.error("Exception in moveFile::"+ex.getMessage());

		}
	}

}
