package com.eiconnector.ExcelImportConnector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import javax.sql.DataSource;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.test.annotation.SystemProfileValueSource;

import com.opencsv.CSVReader;

@SpringBootApplication
@PropertySource("classpath:application.properties")
@EnableScheduling
public class ExcelImportConnectorApplication {
	@Value("${spring.datasource.driver-class-name}")
	private static String driverClassName;
	@Value("${spring.datasource.url}")
	private static String url;
	@Value("${spring.datasource.username}")
	private static String username;
	@Value("${spring.datasource.password}")
	private static String password;
	private Statement statement;
	private static CSVReader readerCSV;
	private static String queryWC;
	private static ResultSet rs;
	private static Connection connection;
	private static final Logger logger = LoggerFactory.getLogger(ExcelImportConnectorApplication.class);

	public static void main(String[] args) throws IOException {

		SpringApplication.run(ExcelImportConnectorApplication.class, args);

		ExcelImportConnectorApplication.readExcel();
	}

	/**
	 * 获取新的数据源
	 *
	 * @return
	 */
	@Bean
	public static DataSource getDataSource() {
		DriverManagerDataSource managerDataSource = new DriverManagerDataSource();
		managerDataSource.setDriverClassName("com.mysql.jdbc.Driver");
		managerDataSource.setPassword("root");
		managerDataSource.setUrl("jdbc:mysql://localhost:3306/ExcelImportConnector");
		managerDataSource.setUsername("root");
		return managerDataSource;
	}

	@SuppressWarnings("resource")
	//@Scheduled(cron = "0 * * * * ?")
	public static void readExcel() throws IOException {

		// file_fscmtopmodelam_finapinvtransactionsam_invoicelinepvo-batch2015581573-20190108_064723.csv
		String path = "D:\\Syam\\Input\\";
		String processedPODir = "D:\\Syam\\Processed\\";
		try {
			File f = new File(path);
			File[] listFiles = f.listFiles();
			for (File file : listFiles) {
				// logger.info("just a test info log");
				String fileName = file.getName();
				String excelFile = path + file.getName();
				logger.info("reader " + excelFile);
				logger.info("reader " + file.getName().substring(0, file.getName().indexOf('.')));
				// Reader reader =
				// Files.newBufferedReader(Paths.get(excelFile));
				// CSVParser csvParser = new CSVParser(reader,
				// CSVFormat.DEFAULT);
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Calendar cal = Calendar.getInstance();
				String time = dateFormat.format(cal.getTime());
				logger.info("Current Time::" + time);
				time = time.replaceAll("\\s+", "_");
				time = time.replaceAll(":", "_");
				final String fileNameWithOutExt = FilenameUtils.removeExtension(fileName);

				int lineNo = 0;
				boolean isTableCreated = false;
				List<String> headers = new ArrayList<String>();
				readerCSV = new CSVReader(new InputStreamReader(new FileInputStream(excelFile)));
				String exactFilename=file.getName().substring(file.getName().indexOf('_')+1, file.getName().lastIndexOf('_'));
				String tableName = exactFilename.substring(exactFilename.indexOf('_')+1, exactFilename.lastIndexOf('_'));
				String headerList[] = readerCSV.readNext();
				while (headerList != null) {
					List<String> values = new ArrayList<String>();
					if (lineNo == 0) {
						headers = Arrays.asList(headerList);
						
						isTableCreated = createTable(tableName, headers);
						lineNo++;
						headerList = readerCSV.readNext();
					} else {
						List<String> vals = Arrays.asList(headerList);
						for (String val : vals) {
							values.add(val.replaceAll("'", ""));
						}
						if (isTableCreated)
							insertRows(tableName, values);
						System.out.println(values);
						headerList = readerCSV.readNext();
					}
				}

				String destinationFileName = processedPODir + fileNameWithOutExt + "_" + time
						+ fileName.substring(fileName.indexOf("."));
				String sourceFileName = file.getAbsolutePath();

				logger.info("destinationFileName" + destinationFileName);
				logger.info("excelFile" + destinationFileName);

				ExcelUtil.moveFile(sourceFileName, destinationFileName);

				/*
				 * for (CSVRecord csvRecord : csvParser) { List<String> values =
				 * new ArrayList<String>(); // Accessing values by the names
				 * assigned to each column if (lineNo == 0) { for (int i = 0; i
				 * < headerList.length; i++) headers.add(csvRecord.get(i));
				 * System.out.println(headers); isTableCreated =
				 * createTable(file.getName().substring(0,
				 * file.getName().indexOf('.')), headers);
				 * 
				 * // Create Table Query
				 * 
				 * lineNo++; } else { for (int i = 0; i < headerList.length;
				 * i++) { values.add(csvRecord.get(i).replaceAll("'", "")); }
				 * 
				 * if (isTableCreated) insertRows(file.getName().substring(0,
				 * file.getName().indexOf('.')), values);
				 * System.out.println(values); } }
				 */
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static boolean createTable(String tableName, List<String> columnNames) {
		Connection conn;
		try {
			conn = getConnection();
			Statement stmt = conn.createStatement();
			//stmt.execute("DROP TABLE IF EXISTS `" + tableName + "`");
			String query = "CREATE TABLE " + tableName + "(";// (NAME
																// varchar(50))
																// "

			logger.info("Create Table");
			queryWC = "(";
			int count = 0;
			for (String column : columnNames) {
				if (columnNames.size() - 1 == count++) {
					query = query + column + " TEXT ";
					queryWC = queryWC + column;
				} else {
					query = query + column + " TEXT, ";
					queryWC = queryWC + column + ", ";
				}
			}
			queryWC = queryWC + ")";
			query = query + ")";
			System.out.println(query);
			DatabaseMetaData meta = conn.getMetaData();
			 ResultSet rs = meta.getTables(null, null, tableName, 
				     new String[] {"TABLE"});
			//rs = stmt.executeQuery("SELECT count(*) as total from lll");
			if(rs.next()==false){
				int no = stmt.executeUpdate(query);
				if (no > 0)
					System.out.println("Table " + tableName + " Created Successfully...");
			}
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	public static void insertRows(String tableName, List<String> values) {
		Connection conn;
		try {
			conn =getConnection();
			Statement stmt = conn.createStatement();
			String query = "INSERT into " + tableName + " " + queryWC + " VALUES (";// (NAME
																					// varchar(50))
																					// "
			int count = 0;
			for (String value : values) {
				if (values.size() - 1 == count++)
					query = query + "\'" + value + "\'";
				else
					query = query + "\'" + value + "\', ";
			}
			query = query + ")";
			System.out.println(query);
			boolean status = stmt.execute(query);
			if (status)
				System.out.println("Table Inserted Successfully...");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	public static Connection getConnection() throws SQLException{
		if(connection == null) return getDataSource().getConnection();
		return connection;
	}
}
