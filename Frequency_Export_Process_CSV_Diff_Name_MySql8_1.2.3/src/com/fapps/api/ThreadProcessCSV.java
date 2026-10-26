package com.fapps.api;

import com.fapps.util.PrintUtilMsg;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.Callable;

public class ThreadProcessCSV implements Callable<String> {
	Events ev = new Events();
	PrintUtilMsg pu = new PrintUtilMsg();
	private String lastProcessedDate = "";
	private boolean anySuccessfulExport = false;
	private final String sno;
	private final String inputDate;
	private final String tableNameTransaction;
	private final String type;
	private final String module;
	private final String circle;
	private final String tech;
	private final String db_URL;
	private final String db_User;
	private final String db_Pass;
	private final String threadName;
	private final String filePath;
	private final String tableNameSchedule;
	private final String db_Name;
	private final String host;
	private final String eventTable;
	private final String maxSno;
	private final String dateColumn;
	private final String dateColumnExits;
	private final int rawLimit;
	private final String logServerIP;
	private final String version;
	private final String serverInfoTable;
	private final String SourceColumn;
	private final String DestColumn;
	private final String queryBase;
	private final String querySuffix;
	private final int date_range;
	private String dbUrl;
	private String dbUser;
	private String dbPass;
	private String host2;
	private String port;
	private String dbname;

	public ThreadProcessCSV(String inputDate, String sno, String tableNameTransaction, String type, String module,
			String circle, String tech, String db_URL, String db_User, String db_Pass, int threadName, String filePath,
			String tableNameSchedule, String db_Name, String host, String eventTable, String maxSno, String dateColumn,
			String dateColumnExits, int rawLimit, String logServerIP, String version, String serverInfoTable,
			String SourceColumn, String DestColumn, String queryBase, String querySuffix, int date_range) {
		this.sno = sno;
		this.inputDate = inputDate;
		this.tableNameTransaction = tableNameTransaction;
		this.type = type;
		this.module = module;
		this.circle = circle;
		this.tech = tech;
		this.db_URL = db_URL;
		this.db_User = db_User;
		this.db_Pass = db_Pass;
		this.threadName = "T" + threadName;
		this.filePath = filePath;
		this.tableNameSchedule = tableNameSchedule;
		this.db_Name = db_Name;
		this.host = host;
		this.eventTable = eventTable;
		this.maxSno = maxSno;
		this.dateColumn = dateColumn;
		this.dateColumnExits = dateColumnExits;
		this.rawLimit = rawLimit;
		this.logServerIP = logServerIP;
		this.version = version;
		this.serverInfoTable = serverInfoTable;
		this.SourceColumn = SourceColumn;
		this.DestColumn = DestColumn;
		this.queryBase = queryBase;
		this.querySuffix = querySuffix;
		this.date_range = date_range;
	}

	@Override
	public String call() throws Exception {
		String final_data = "";
		String response = "error";
		System.out.println("ThreadProcessCSV:::: Starting " + threadName);
		System.out.println("Time:" + currentTime() + " [Debug] Process_Name: EP Server_IP: " + this.logServerIP
				+ " Process_Version: " + this.version
				+ " Log_Level: EVT_LOG  EVT_Type: Thread_Start Description: tableName:" + this.tableNameTransaction
				+ "#threadName:" + threadName);
		String start_time = currentTime();
		String status = createCSVViaLoadCommand();
		System.out.println("[" + threadName + "] final status is " + status);
		if (status.split("EPBreak")[0].equals("true")) {
			response = "success";
		} else if (status.split("EPBreak")[0].equals("true_0")) {
			response = "success_0";
		} else {
			response = "failed";
		}
		System.out.println("Time:" + currentTime() + " [Debug] Process_Name: EP Server_IP: " + this.logServerIP
				+ " Process_Version: " + this.version
				+ " Log_Level: EVT_LOG  EVT_Type: Thread_Status Description: tableName:" + this.tableNameTransaction
				+ "#threadName:" + threadName + "#response:" + response);
		final_data = threadName + "," + response + "," + this.tableNameTransaction + "," + this.sno + "," + this.type
				+ "," + this.module + "," + this.circle + "," + this.tech + "," + start_time + ","
				+ status.split("EPBreak")[1];
		updateStatusAndEvent(final_data);
		System.out.println("Time:" + currentTime() + " [Debug] Process_Name: EP Server_IP: " + this.logServerIP
				+ " Process_Version: " + this.version
				+ " Log_Level: EVT_LOG  EVT_Type: Thread_End Description: tableName:" + this.tableNameTransaction
				+ "#threadName:" + threadName);
		return final_data;
	}

	public String[] getServerInfo(String serverInfoTable, String db_URL, String db_User, String db_Pass) {
		String[] serverInfo = new String[6];
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String query = "select dburl, dbuser, dbpass, dbhost, dbport, dbname from " + serverInfoTable
					+ " where server_status = 'A'";
			conn = new com.fapps.db.MySQLConnection().getConnection(db_URL, db_User, db_Pass);
			ps = conn.prepareStatement(query);
			rs = ps.executeQuery();
			if (rs.next()) {
				serverInfo[0] = rs.getString("dburl");
				serverInfo[1] = rs.getString("dbuser");
				serverInfo[2] = rs.getString("dbpass");
				serverInfo[3] = rs.getString("dbhost");
				serverInfo[4] = rs.getString("dbport");
				serverInfo[5] = rs.getString("dbname");
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		} finally {
			try {
				if (conn != null) {
					if (rs != null)
						rs.close();
					if (ps != null)
						ps.close();
					conn.close();
				}
			} catch (Exception e) {
			}
		}
		return serverInfo;
	}

	private String createCSVViaLoadCommand() {
		String status = "true_0";
		long totalRecords = 0;

		try {
			String[] serverinfo = getServerInfo(this.serverInfoTable, this.db_URL, this.db_User, this.db_Pass);
			if (serverinfo != null) {
				this.dbUrl = serverinfo[0];
				this.dbUser = serverinfo[1];
				this.dbPass = serverinfo[2];
				this.host2 = serverinfo[3];
				this.port = serverinfo[4];
				this.dbname = serverinfo[5];
			} else {
				System.out.println("[" + threadName + "] ThreadProcessCSV:::: createCSVViaLoadCommand:::: Server info not found");
				return "falseEPBreak0";
			}

			System.out.println(serverinfo[0] + "--" + serverinfo[1] + "--" + serverinfo[2] + "--" + serverinfo[3] + "--"
					+ serverinfo[4] + "--" + serverinfo[5]);

			String[] SourceColumnNames = this.SourceColumn.split(",");
			String[] DestColumnNames = this.DestColumn.split(",");

			String columnNamesHeader = "'" + String.join(",", DestColumnNames).replaceAll(",", "','") + "'";
			String typeValue = this.type.equalsIgnoreCase("r") ? "raw" : "process";
			String sourceColumnSql = String.join(",", SourceColumnNames);
			String baseFrom = (this.queryBase == null || this.queryBase.trim().isEmpty()
					|| this.queryBase.equalsIgnoreCase("null")) ? this.tableNameTransaction : this.queryBase;
			String suffix = (this.querySuffix == null || this.querySuffix.trim().isEmpty()
					|| this.querySuffix.equalsIgnoreCase("null")) ? "" : " " + this.querySuffix;

			Date startDate = parseDate(this.maxSno);
			if (startDate == null) {
				System.out.println("[" + threadName + "] createCSVViaLoadCommand: Unable to parse maxSno as date: " + this.maxSno);
				return "falseEPBreak0";
			}

			if (this.date_range <= 0) {
				System.out.println("[" + threadName + "] Invalid date_range: " + this.date_range);
				return "falseEPBreak0";
			}

			Calendar cal = Calendar.getInstance();
			cal.setTime(startDate);

			// one unique stamp for this thread run
			String runStamp = time();

			for (int day = 0; day < this.date_range; day++) {
				cal.add(Calendar.DATE, 1);
				String currentDateStr = formatDate(cal.getTime());

				ev.insertEpEvents(currentDateStr, "NA", this.eventTable, this.threadName, this.type, this.module,
						this.circle, this.tech, this.tableNameTransaction, this.sno, currentTime(), "NA", "event_start",
						this.db_URL, this.db_User, this.db_Pass);

				String input_date = currentDateStr.replaceAll("-", "_");
				String path = filePath + "/" + typeValue + "/" + typeValue + "_" + this.module + "_" + this.circle + "_"
						+ this.tech + "_" + this.sno + "_" + input_date + "_" + runStamp + ".csv";
				String path1 = filePath + "/" + typeValue + "/" + typeValue + "_" + this.module + "_" + this.circle
						+ "_" + this.tech + "_" + this.sno + "_" + input_date + "*";

				System.out.println("[" + threadName + "] ThreadProcessCSV:::: In createCSVViaLoadCommand::: sno["
						+ this.sno + "], Table_name[" + this.tableNameTransaction + "], type[" + this.type
						+ "], module[" + this.module + "], circle[" + this.circle + "], tech[" + this.tech + "]");
				System.out.println("[" + threadName + "] Processing date: " + currentDateStr);
				System.out.println("[" + threadName + "] ThreadProcessCSV:::: In createCSVViaLoadCommand::: " + path);
				System.out.println("Time:" + currentTime() + " [Debug] Process_Name: EP Server_IP: " + this.logServerIP
						+ " Process_Version: " + this.version
						+ " Log_Level: EVT_LOG  EVT_Type: fileName Description: tableName:" + this.tableNameTransaction
						+ "#threadName:" + threadName + "#fileName:" + path);

				Process p2 = null;
				try {
					p2 = Runtime.getRuntime().exec(new String[] { "sh", "remove_file.sh", path1 });
					int exitCode = p2.waitFor();
					System.out.println("[" + threadName + "] ThreadProcessCSV:::: In createCSVViaLoadCommand::: remove_file exitCode " + exitCode);
				} catch (Exception ee) {
					ee.printStackTrace();
				} finally {
					try {
						if (p2 != null) p2.destroy();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				String query = buildDTQuery(serverinfo, columnNamesHeader, sourceColumnSql, baseFrom, currentDateStr, suffix, path);
				System.out.println("query to create file " + query);

				Process p = null;
				boolean fileWritten = false;
				try {
					p = Runtime.getRuntime().exec(new String[] { "bash", "-c", query });
					int exitCode = p.waitFor();

					// print mysql stderr if any
					BufferedReader errBr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
					StringBuilder errMsg = new StringBuilder();
					String errLine;
					while ((errLine = errBr.readLine()) != null) {
						errMsg.append(errLine).append(" | ");
					}
					if (errMsg.length() > 0) {
						System.out.println("[" + threadName + "] mysql stderr: " + errMsg);
					}

					java.io.File exportedFile = new java.io.File(path);
					System.out.println("[" + threadName + "] fileExists=" + exportedFile.exists()
							+ " fileSize=" + (exportedFile.exists() ? exportedFile.length() : 0)
							+ " exitCode=" + exitCode);

					if (exitCode == 0 && exportedFile.exists() && exportedFile.length() > 0) {
						System.out.println("[" + threadName + "] Success: file written for " + currentDateStr);
						System.out.println("Time:" + currentTime() + " [Debug] Process_Name: EP Server_IP: "
								+ this.logServerIP + " Process_Version: " + this.version
								+ " Log_Level: EVT_LOG  EVT_Type: FileCreate Description: tableName:"
								+ this.tableNameTransaction + "#threadName:" + threadName + "#Success: file written");
						fileWritten = true;
					} else {
						System.out.println("[" + threadName + "] mysql export failed for date " + currentDateStr);
					}
				} catch (Exception ein) {
					System.out.println("Time:" + currentTime() + " [Debug] Process_Name: EP Server_IP: "
							+ this.logServerIP + " Process_Version: " + this.version
							+ " Log_Level: Error  EVT_Type: FileCreate Description: tableName:"
							+ this.tableNameTransaction + "#threadName:" + threadName + "#Fail: file written for date "
							+ currentDateStr);
					ein.printStackTrace();
				} finally {
					try {
						if (p != null) p.destroy();
					} catch (Exception ee) {
						ee.printStackTrace();
					}
				}

				if (!fileWritten) {
					status = "false";
					break;
				}

				String lineData;
				Process p1 = null;
				BufferedReader br = null;
				boolean dayProcessed = false;
				try {
					java.io.File exportedFile = new java.io.File(path);
					if (!exportedFile.exists() || exportedFile.length() == 0) {
						System.out.println("[" + threadName + "] Exported file not found or empty: " + path);
						status = "false";
						break;
					}

					p1 = Runtime.getRuntime().exec(new String[] { "wc", "-l", path });
					p1.waitFor();
					br = new BufferedReader(new InputStreamReader(p1.getInputStream()));

					while ((lineData = br.readLine()) != null) {
						System.out.println("line: " + lineData);
						int lineCount = Integer.parseInt(lineData.trim().split("\\s+")[0]);
						if (lineCount > 1) {
							int actualRecords = lineCount - 1;
							totalRecords += actualRecords;
							System.out.println("File Count is: " + actualRecords + " for date " + currentDateStr);
							System.out.println("Time:" + currentTime() + " [Debug] Process_Name: EP Server_IP: "
									+ this.logServerIP + " Process_Version: " + this.version
									+ " Log_Level: EVT_LOG  EVT_Type: rawCount Description: tableName:"
									+ this.tableNameTransaction + "#threadName:" + threadName + "#rawCount:"
									+ actualRecords);
							status = "true";
							anySuccessfulExport = true;
							dayProcessed = true;
							updateMaxSnoStr(this.db_URL, this.db_User, this.db_Pass, this.tableNameTransaction,
									this.threadName, this.tableNameSchedule, currentDateStr);
							lastProcessedDate = currentDateStr;
						} else {
							System.out.println("File Count is: " + lineCount + " for date " + currentDateStr);
							System.out.println("Time:" + currentTime() + " [Debug] Process_Name: EP Server_IP: "
									+ this.logServerIP + " Process_Version: " + this.version
									+ " Log_Level: EVT_LOG  EVT_Type: rawCount Description: tableName:"
									+ this.tableNameTransaction + "#threadName:" + threadName + "#rawCount:0");
							dayProcessed = true;
							if (!anySuccessfulExport) {
								status = "true_0";
							}
							updateMaxSnoStr(this.db_URL, this.db_User, this.db_Pass, this.tableNameTransaction,
									this.threadName, this.tableNameSchedule, currentDateStr);
							lastProcessedDate = currentDateStr;
						}
					}
					System.out.println("exit: " + p1.exitValue());
				} catch (Exception e) {
					System.out.println("Exception while reading CSV line count for date " + currentDateStr + " " + e.toString());
					System.out.println("Time:" + currentTime() + " [Debug] Process_Name: EP Server_IP: "
							+ this.logServerIP + " Process_Version: " + this.version
							+ " Log_Level: Error  EVT_Type: rawCount Description: tableName:"
							+ this.tableNameTransaction + "#threadName:" + threadName);
					e.printStackTrace();
				} finally {
					try {
						if (br != null) br.close();
						if (p1 != null) p1.destroy();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				if (!dayProcessed) {
					status = "false";
					break;
				}
			}

		} catch (Exception e) {
			status = "false";
			System.out.print("ThreadProcessCSV:::: createCSVViaLoadCommand:::: Exception while processing CSV " + e.toString());
			e.printStackTrace();
		}
		return status + "EPBreak" + totalRecords;
	}
	
	
//	private String createCSVViaLoadCommand() {
//		String status = "true_0";
//		long totalRecords = 0;
//		try {
//			String[] serverinfo = getServerInfo(this.serverInfoTable, this.db_URL, this.db_User, this.db_Pass);
//			if (serverinfo != null) {
//				this.dbUrl = serverinfo[0];
//				this.dbUser = serverinfo[1];
//				this.dbPass = serverinfo[2];
//				this.host2 = serverinfo[3];
//				this.port = serverinfo[4];
//				this.dbname = serverinfo[5];
//			} else {
//				System.out.println(
//						"[" + threadName + "] ThreadProcessCSV:::: createCSVViaLoadCommand:::: Server info not found");
//				return "falseEPBreak0";
//			}
//			System.out.println(serverinfo[0] + "--" + serverinfo[1] + "--" + serverinfo[2] + "--" + serverinfo[3] + "--"
//					+ serverinfo[4] + "--" + serverinfo[5]);
//
//			String[] SourceColumnNames = this.SourceColumn.split(",");
//			String[] DestColumnNames = this.DestColumn.split(",");
//
//			String columnNamesHeader = "'" + String.join(",", DestColumnNames).replaceAll(",", "','") + "'";
//			String typeValue = this.type.equalsIgnoreCase("r") ? "raw" : "process";
//			String sourceColumnSql = String.join(",", SourceColumnNames);
//			String baseFrom = (this.queryBase == null || this.queryBase.trim().isEmpty()
//					|| this.queryBase.equalsIgnoreCase("null")) ? this.tableNameTransaction : this.queryBase;
//			String suffix = (this.querySuffix == null || this.querySuffix.trim().isEmpty()
//					|| this.querySuffix.equalsIgnoreCase("null")) ? "" : " " + this.querySuffix;
//
//			Date startDate = parseDate(this.maxSno);
//			if (startDate == null) {
//				System.out.println(
//						"[" + threadName + "] createCSVViaLoadCommand: Unable to parse maxSno as date: " + this.maxSno);
//				return "falseEPBreak0";
//			}
//
//			if (this.date_range <= 0) {
//				System.out.println("[" + threadName + "] Invalid date_range: " + this.date_range);
//				return "falseEPBreak0";
//			}
//
//			Calendar cal = Calendar.getInstance();
//			cal.setTime(startDate);
//
//			for (int day = 0; day < this.date_range; day++) {
//				cal.add(Calendar.DATE, 1);
//				String currentDateStr = formatDate(cal.getTime());
//
//				ev.insertEpEvents(currentDateStr, "NA", this.eventTable, this.threadName, this.type, this.module,
//						this.circle, this.tech, this.tableNameTransaction, this.sno, currentTime(), "NA", "event_start",
//						this.db_URL, this.db_User, this.db_Pass);
//
//				String input_date = currentDateStr.replaceAll("-", "_");
//				String path = filePath + "/" + typeValue + "/" + typeValue + "_" + this.module + "_" + this.circle + "_"
//						+ this.tech + "_" + this.sno + "_" + input_date + "_" + time() + ".csv";
//				String path1 = filePath + "/" + typeValue + "/" + typeValue + "_" + this.module + "_" + this.circle
//						+ "_" + this.tech + "_" + this.sno + "_" + input_date + "*";
//
//				System.out.println("[" + threadName + "] ThreadProcessCSV:::: In createCSVViaLoadCommand::: sno["
//						+ this.sno + "], Table_name[" + this.tableNameTransaction + "], type[" + this.type
//						+ "], module[" + this.module + "], circle[" + this.circle + "], tech[" + this.tech + "]");
//				System.out.println("[" + threadName + "] Processing date: " + currentDateStr);
//				System.out.println("[" + threadName + "] ThreadProcessCSV:::: In createCSVViaLoadCommand::: " + path);
//				System.out.println("Time:" + currentTime() + " [Debug] Process_Name: EP Server_IP: " + this.logServerIP
//						+ " Process_Version: " + this.version
//						+ " Log_Level: EVT_LOG  EVT_Type: fileName Description: tableName:" + this.tableNameTransaction
//						+ "#threadName:" + threadName + "#fileName:" + path);
//
//				Process p2 = null;
//				try {
//					p2 = Runtime.getRuntime().exec(new String[] { "sh", "remove_file.sh", path1 });
//					int exitCode = p2.waitFor();
//					System.out.println("[" + threadName
//							+ "] ThreadProcessCSV:::: In createCSVViaLoadCommand::: exitCode" + exitCode);
//				} catch (Exception ee) {
//					ee.printStackTrace();
//				} finally {
//					try {
//						if (p2 != null)
//							p2.destroy();
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//				}
//
//				String query = buildDTQuery(serverinfo, columnNamesHeader, sourceColumnSql, baseFrom, currentDateStr,
//						suffix, path);
//				System.out.println("query to create file " + query);
//
//				Process p = null;
//				boolean fileWritten = false;
//				try {
//					p = Runtime.getRuntime().exec(new String[] { "bash", "-c", query });
//					int exitCode = p.waitFor();
//					if (exitCode == 0) {
//						System.out.println("[" + threadName + "] Success: file written for " + currentDateStr);
//						System.out.println("Time:" + currentTime() + " [Debug] Process_Name: EP Server_IP: "
//								+ this.logServerIP + " Process_Version: " + this.version
//								+ " Log_Level: EVT_LOG  EVT_Type: FileCreate Description: tableName:"
//								+ this.tableNameTransaction + "#threadName:" + threadName + "#Success: file written");
//						fileWritten = true;
//					} else {
//						System.out.println("[" + threadName + "] mysql export failed with exitCode=" + exitCode
//								+ " for date " + currentDateStr);
//					}
//				} catch (Exception ein) {
//					System.out.println("Time:" + currentTime() + " [Debug] Process_Name: EP Server_IP: "
//							+ this.logServerIP + " Process_Version: " + this.version
//							+ " Log_Level: Error  EVT_Type: FileCreate Description: tableName:"
//							+ this.tableNameTransaction + "#threadName:" + threadName + "#Fail: file written for date "
//							+ currentDateStr);
//					ein.printStackTrace();
//				} finally {
//					try {
//						if (p != null)
//							p.destroy();
//					} catch (Exception ee) {
//						ee.printStackTrace();
//					}
//				}
//
//				if (!fileWritten) {
//					status = "false";
//					break;
//				}
//
//				String lineData;
//				Process p1 = null;
//				BufferedReader br = null;
//				boolean dayProcessed = false;
//				try {
//					java.io.File exportedFile = new java.io.File(path);
//					if (!exportedFile.exists()) {
//						System.out.println("[" + threadName + "] Exported file not found: " + path);
//						status = "false";
//						break;
//					}
//
//					// FIX: array form so paths with spaces are handled
//					// correctly
//					p1 = Runtime.getRuntime().exec(new String[] { "wc", "-l", path });
//					p1.waitFor();
//					br = new BufferedReader(new InputStreamReader(p1.getInputStream()));
//					while ((lineData = br.readLine()) != null) {
//						System.out.println("line: " + lineData);
//						int lineCount = Integer.parseInt(lineData.trim().split("\\s+")[0]);
//						if (lineCount > 1) {
//							int actualRecords = lineCount - 1;
//							totalRecords += actualRecords;
//							System.out.println("File Count is: " + actualRecords + " for date " + currentDateStr);
//							System.out.println("Time:" + currentTime() + " [Debug] Process_Name: EP Server_IP: "
//									+ this.logServerIP + " Process_Version: " + this.version
//									+ " Log_Level: EVT_LOG  EVT_Type: rawCount Description: tableName:"
//									+ this.tableNameTransaction + "#threadName:" + threadName + "#rawCount:"
//									+ actualRecords);
//							status = "true";
//							anySuccessfulExport = true;
//							dayProcessed = true;
//							updateMaxSnoStr(this.db_URL, this.db_User, this.db_Pass, this.tableNameTransaction,
//									this.threadName, this.tableNameSchedule, currentDateStr);
//							lastProcessedDate = currentDateStr;
//						} else {
//							System.out.println("File Count is: " + lineCount + " for date " + currentDateStr);
//							System.out.println("Time:" + currentTime() + " [Debug] Process_Name: EP Server_IP: "
//									+ this.logServerIP + " Process_Version: " + this.version
//									+ " Log_Level: EVT_LOG  EVT_Type: rawCount Description: tableName:"
//									+ this.tableNameTransaction + "#threadName:" + threadName + "#rawCount:0");
//							dayProcessed = true;
//							if (!anySuccessfulExport) {
//								status = "true_0";
//							}
//							updateMaxSnoStr(this.db_URL, this.db_User, this.db_Pass, this.tableNameTransaction,
//									this.threadName, this.tableNameSchedule, currentDateStr);
//							lastProcessedDate = currentDateStr;
//						}
//					}
//					System.out.println("exit: " + p1.exitValue());
//				} catch (Exception e) {
//					System.out.println(
//							"Exception while reading CSV line count for date " + currentDateStr + " " + e.toString());
//					System.out.println("Time:" + currentTime() + " [Debug] Process_Name: EP Server_IP: "
//							+ this.logServerIP + " Process_Version: " + this.version
//							+ " Log_Level: Error  EVT_Type: rawCount Description: tableName:"
//							+ this.tableNameTransaction + "#threadName:" + threadName);
//					e.printStackTrace();
//				} finally {
//					try {
//						if (br != null)
//							br.close();
//						if (p1 != null)
//							p1.destroy();
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//				}
//
//				if (!dayProcessed) {
//					status = "false";
//					break;
//				}
//			}
//
//		} catch (Exception e) {
//			status = "false";
//			System.out.print(
//					"ThreadProcessCSV:::: createCSVViaLoadCommand:::: Exception while processing CSV " + e.toString());
//			e.printStackTrace();
//		}
//		return status + "EPBreak" + totalRecords;
//	}
	private String buildDTQuery(String[] serverinfo, String columnNamesHeader, String sourceColumnSql, String baseFrom,
			String dateStr, String suffix, String path) {

		String mode = this.dateColumnExits == null ? "" : this.dateColumnExits.trim().toUpperCase();
		String whereCondition = buildQueryCondition(mode, this.dateColumn, dateStr, baseFrom);

		return "mysql -u" + serverinfo[1]
				+ " --password=\"" + serverinfo[2] + "\""
				+ " -h" + serverinfo[3]
				+ " -P " + serverinfo[4]
				+ " " + serverinfo[5]
				+ " -s -N -e \"select " + columnNamesHeader
				+ " union all (select " + sourceColumnSql
				+ " from " + baseFrom
				+ " where " + whereCondition + suffix
				+ ")\" | sed 's/\"/\\\\\"/g;s/\\t/\"|\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;s/\"NULL\"/\\\\N/g;' "
				+ "| tr -d '\\r' >> '" + path + "'";
	}
	private String buildQueryCondition(String mode, String columnName, String dateStr, String baseFrom) {
		if (mode == null || mode.trim().isEmpty()) {
			return "1=1";
		}
		String dataType = getColumnSqlDataType(this.db_URL, this.db_User, this.db_Pass, baseFrom, columnName);
		String normalizedType = dataType == null ? "" : dataType.trim().toLowerCase();

		boolean varcharLike =
				normalizedType.contains("char")
				|| normalizedType.contains("text")
				|| normalizedType.contains("enum")
				|| normalizedType.contains("set");

		switch (mode.trim().toUpperCase()) {
			case "DT1":
			case "DT3":
				if (varcharLike) {
					String storedFormat = getStoredDateFormat(this.db_URL, this.db_User, this.db_Pass, baseFrom, columnName);
					if (storedFormat.equalsIgnoreCase("dd-MM-yyyy")) {
						return columnName + " = '" + convertDate(dateStr, "yyyy-MM-dd", "dd-MM-yyyy") + "'";
					} else if (storedFormat.equalsIgnoreCase("yyyy/MM/dd")) {
						return columnName + " = '" + convertDate(dateStr, "yyyy-MM-dd", "yyyy/MM/dd") + "'";
					} else if (storedFormat.equalsIgnoreCase("yyyy-MM-dd")) {
						return columnName + " = '" + dateStr + "'";
					} else if (storedFormat.equalsIgnoreCase("yyyy-MM-dd HH:mm:ss")
							|| storedFormat.equalsIgnoreCase("yyyy-MM-dd HH:mm:ss.SSS")) {
						return columnName + " >= '" + dateStr + " 00:00:00' "
								+ "AND " + columnName + " < DATE_ADD('" + dateStr + "', INTERVAL 1 DAY)";
					}
				}
				return columnName + " >= '" + dateStr + "' "
						+ "AND " + columnName + " < DATE_ADD('" + dateStr + "', INTERVAL 1 DAY)";

			case "DT2":
				if (varcharLike) {
					String storedFormat = getStoredDateFormat(this.db_URL, this.db_User, this.db_Pass, baseFrom, columnName);
					if (storedFormat.equalsIgnoreCase("dd-MM-yyyy")) {
						return columnName + " = '" + convertDate(dateStr, "yyyy-MM-dd", "dd-MM-yyyy") + "'";
					} else if (storedFormat.equalsIgnoreCase("yyyy/MM/dd")) {
						return columnName + " = '" + convertDate(dateStr, "yyyy-MM-dd", "yyyy/MM/dd") + "'";
					} else if (storedFormat.equalsIgnoreCase("yyyy-MM-dd")) {
						return columnName + " = '" + dateStr + "'";
					} else {
						return columnName + " = '" + convertDate(dateStr, "yyyy-MM-dd", "dd-MM-yyyy") + "'";
					}
				}
				return columnName + " >= '" + dateStr + "' "
						+ "AND " + columnName + " < DATE_ADD('" + dateStr + "', INTERVAL 1 DAY)";

			case "DT4":
			case "DT5":
				return "1=1";

			default:
				return columnName + " >= '" + dateStr + "' "
						+ "AND " + columnName + " < DATE_ADD('" + dateStr + "', INTERVAL 1 DAY)";
		}
	}
	private String getColumnSqlDataType(String db_url, String db_User, String db_pass, String table, String columnName) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String schema = table;
			String tbl = table;
			if (table.contains(".")) {
				String[] parts = table.split("\\.", 2);
				schema = parts[0];
				tbl = parts[1];
			}

			String query = "select DATA_TYPE from INFORMATION_SCHEMA.COLUMNS "
					+ "where TABLE_SCHEMA=? and TABLE_NAME=? and COLUMN_NAME=?";

			conn = new com.fapps.db.MySQLConnection().getConnection(db_url, db_User, db_pass);
			ps = conn.prepareStatement(query);
			ps.setString(1, schema);
			ps.setString(2, tbl);
			ps.setString(3, columnName);

			rs = ps.executeQuery();
			if (rs.next()) {
				return rs.getString(1);
			}
		} catch (Exception ex) {
			System.out.println("[" + threadName + "] EXCEPTION JAR: getColumnSqlDataType: " + ex.toString());
		} finally {
			try {
				if (conn != null) {
					if (rs != null) rs.close();
					if (ps != null) ps.close();
					conn.close();
				}
			} catch (Exception e) {
			}
		}
		return "";
	}
	private String getStoredDateFormat(String db_url, String db_User, String db_pass, String table, String columnName) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String q = "select " + columnName + " from " + table
					+ " where " + columnName + " is not null and trim(" + columnName + ") <> '' limit 1";

			conn = new com.fapps.db.MySQLConnection().getConnection(db_url, db_User, db_pass);
			ps = conn.prepareStatement(q);
			rs = ps.executeQuery();

			if (rs.next()) {
				String v = rs.getString(1);
				if (v == null) return "";
				v = v.trim();

				if (v.matches("\\d{2}-\\d{2}-\\d{4}")) return "dd-MM-yyyy";
				if (v.matches("\\d{4}-\\d{2}-\\d{2}$")) return "yyyy-MM-dd";
				if (v.matches("\\d{4}/\\d{2}/\\d{2}$")) return "yyyy/MM/dd";
				if (v.matches("\\d{2}/\\d{2}/\\d{4}$")) return "dd/MM/yyyy";
				if (v.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{1,3}")) return "yyyy-MM-dd HH:mm:ss.SSS";
				if (v.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) return "yyyy-MM-dd HH:mm:ss";
				if (v.matches("\\d{8}")) return "yyyyMMdd";
			}
		} catch (Exception ex) {
			System.out.println("[" + threadName + "] EXCEPTION JAR: getStoredDateFormat: " + ex.toString());
		} finally {
			try {
				if (conn != null) {
					if (rs != null) rs.close();
					if (ps != null) ps.close();
					conn.close();
				}
			} catch (Exception e) {
			}
		}
		return "";
	}
	private String convertDate(String inputDate, String inputFormat, String outputFormat) {

		try {

			java.text.SimpleDateFormat in = new java.text.SimpleDateFormat(inputFormat);

			java.text.SimpleDateFormat out = new java.text.SimpleDateFormat(outputFormat);

			return out.format(in.parse(inputDate));

		} catch (Exception e) {

			return inputDate;
		}
	}

	private Date parseDate(String dateStr) {
		if (dateStr == null || dateStr.trim().isEmpty()) {
			return null;
		}
		dateStr = dateStr.trim();
		String[] formats = { "yyyy-MM-dd", "yyyy/MM/dd", "dd-MM-yyyy", "dd/MM/yyyy", "MM-dd-yyyy", "MM/dd/yyyy",
				"yyyyMMdd", "yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss", "dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss",
				"MM-dd-yyyy HH:mm:ss", "MM/dd/yyyy HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS",
				"yyyy-MM-dd'T'HH:mm:ssXXX", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX" };
		for (String format : formats) {
			try {
				SimpleDateFormat sdf = new SimpleDateFormat(format);
				sdf.setLenient(false);
				return sdf.parse(dateStr);
			} catch (Exception ignored) {
			}
		}
		try {
			if (dateStr.contains(" ")) {
				String firstPart = dateStr.split("\\s+")[0];
				String[] fallbackFormats = { "yyyy-MM-dd", "yyyy/MM/dd", "dd-MM-yyyy", "dd/MM/yyyy", "MM-dd-yyyy",
						"MM/dd/yyyy", "yyyyMMdd" };
				for (String format : fallbackFormats) {
					try {
						SimpleDateFormat sdf = new SimpleDateFormat(format);
						sdf.setLenient(false);
						return sdf.parse(firstPart);
					} catch (Exception ignored) {
					}
				}
			}
		} catch (Exception ignored) {
		}
		System.out.println("[" + threadName + "] parseDate: Unsupported format: " + dateStr);
		return null;
	}

	private String formatDate(Date date) {
		return new SimpleDateFormat("yyyy-MM-dd").format(date);
	}

	private void updateMaxSnoStr(String db_URL, String db_User, String db_Pass, String tableNameTransaction,
			String threadName, String tableNameSchedule, String dateValue) {
		Connection conn = null;
		PreparedStatement ps = null;
		String query = null;
		try {
			query = "update " + tableNameSchedule + " set maxSno=? where table_name=?";
			conn = new com.fapps.db.MySQLConnection().getConnection(db_URL, db_User, db_Pass);
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(query);
			ps.setString(1, dateValue);
			ps.setString(2, tableNameTransaction);
			ps.addBatch();
			pu.sop("[" + threadName + "] update maxSno query after batch addition", ps.toString());
			ps.executeBatch();
			conn.commit();
		} catch (Exception ex) {
			System.out.println(
					"[" + threadName + "] EXCEPTION JAR: updateMaxSnoStr: [" + threadName + "]" + ex.toString());
			ex.printStackTrace();
		} finally {
			try {
				if (conn != null) {
					if (ps != null)
						ps.close();
					conn.close();
				}
			} catch (Exception e) {
			}
		}
	}

	private void updateMaxSno(String db_URL, String db_User, String db_Pass, String tableNameTransaction,
			String threadName, String tableNameSchedule, long tableMaxSno) {
		Connection conn = null;
		PreparedStatement ps = null;
		String query = null;
		try {
			query = "update " + tableNameSchedule + " set maxSno=? where table_name=?";
			conn = new com.fapps.db.MySQLConnection().getConnection(db_URL, db_User, db_Pass);
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(query);
			ps.setString(1, Long.toString(tableMaxSno));
			ps.setString(2, tableNameTransaction);
			ps.addBatch();
			pu.sop("[" + threadName + "] update query after batch addition", ps.toString());
			ps.executeBatch();
			conn.commit();
		} catch (Exception ex) {
			System.out
					.println("[" + threadName + "] EXCEPTION JAR: updateMaxSno: [" + threadName + "]" + ex.toString());
			ex.printStackTrace();
		} finally {
			try {
				if (conn != null) {
					if (ps != null)
						ps.close();
					conn.close();
				}
			} catch (Exception e) {
			}
		}
	}

	private long getTableMaxSno(String db_url, String db_User, String db_pass, String table, String threadName,
			String dateColumn, String dateColumnExits) {
		long response = 0;
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String query = null;
		int rowCount = 0;
		try {
			if (dateColumnExits.equalsIgnoreCase("Y") || dateColumnExits.equalsIgnoreCase("AT")
					|| dateColumnExits.equalsIgnoreCase("VT") || dateColumnExits.equalsIgnoreCase("LT")) {
				query = "select max(" + dateColumn + ") as sno from " + table;
			} else {
				return 0;
			}
			System.out.println("[" + threadName + "] Query API Log:  getTableMaxSno: " + query);
			conn = new com.fapps.db.MySQLConnection().getConnection(db_url, db_User, db_pass);
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(query);
			rs.last();
			rowCount = rs.getRow();
			rs.beforeFirst();
			if (rowCount > 0) {
				while (rs.next()) {
					response = rs.getLong("sno");
				}
			}
			System.out.println("[" + threadName + "] MaxSno: " + response);
		} catch (Exception ex) {
			System.out.println("[" + threadName + "] EXCEPTION JAR: ::  getTableMaxSno: " + ex.toString());
			response = 0;
			ex.printStackTrace();
		} finally {
			try {
				if (conn != null) {
					if (rs != null)
						rs.close();
					if (stmt != null)
						stmt.close();
					conn.close();
				}
			} catch (Exception e) {
			}
		}
		return response;
	}

	private String[] getTableColumnName(String db_url, String db_User, String db_pass, String table, String threadName,
			String columnName, String dateColumnExits) {
		String[] response = null;
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String query = null;
		try {
			query = "select * from " + table + " limit 1";
			System.out.println("[" + threadName + "] Query API Log:  getTableColumnName: " + query);
			System.out.println("[" + threadName + "] Query API Log:  getTableColumnName: " + db_url + "," + db_User
					+ "," + db_pass);
			conn = new com.fapps.db.MySQLConnection().getConnection(db_url, db_User, db_pass);
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(query);
			if (rs != null) {
				response = new String[2];
				ResultSetMetaData columns = rs.getMetaData();
				int i = 0;
				while (i < columns.getColumnCount()) {
					i++;
					System.out.print(columns.getColumnName(i) + "\t");
					if (this.dateColumnExits.equalsIgnoreCase("Y")) {
						if (!columns.getColumnName(i).equals(columnName)) {
							response[0] = response[0] + "," + columns.getColumnName(i);
							response[1] = response[1] + "," + "replace(" + columns.getColumnName(i)
									+ ",'\\\"',\\\"'\\\")";
						}
					} else {
						response[0] = response[0] + "," + columns.getColumnName(i);
						response[1] = response[1] + "," + "replace(" + columns.getColumnName(i) + ",'\\\"',\\\"'\\\")";
					}
				}
				System.out.println("[" + threadName + "] i: " + i);
			}
			response[0] = response[0].replace("null", "");
			response[1] = response[1].replace("null", "");
			response[0] = response[0].substring(1);
			response[1] = response[1].substring(1);
			System.out.println("[" + threadName + "] Columns are: " + response[0]);
			System.out.println("[" + threadName + "] Columns are: " + response[1]);
		} catch (Exception ex) {
			System.out.println("[" + threadName + "] EXCEPTION JAR: ::  getTableColumnName: " + ex.toString());
			response = null;
			ex.printStackTrace();
		} finally {
			try {
				if (conn != null) {
					if (rs != null)
						rs.close();
					if (stmt != null)
						stmt.close();
					conn.close();
				}
			} catch (Exception e) {
			}
		}
		return response;
	}

	private String currentTime() {
		String current_time = "";
		try {
			SimpleDateFormat sdf_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Calendar c1 = Calendar.getInstance();
			Date date = new Date();
			c1.setTime(date);
			current_time = sdf_date.format(c1.getTime());
		} catch (Exception E) {
			System.out.println("currentTime::   Exception while computing current date");
			E.printStackTrace();
		}
		return current_time;
	}

	private String time() {
		String current_time = "";
		try {
			// SSS (milliseconds) added so each day's file gets a unique
			// timestamp even within the same second
			SimpleDateFormat sdf_date = new SimpleDateFormat("HH_mm_ss_SSS");
			Calendar c1 = Calendar.getInstance();
			Date date = new Date();
			c1.setTime(date);
			current_time = sdf_date.format(c1.getTime());
		} catch (Exception E) {
			System.out.println("currentTime::   Exception while computing current date");
			E.printStackTrace();
		}
		return current_time;
	}

	private void updateStatusAndEvent(String final_data) {
		try {
			String updateScheduleTableStatus = "true";
			System.out.println(
					"[" + final_data.split(",")[0] + "] processScheduledTransactions:::: Response from CSV Thread::::: "
							+ new Date() + " :::: " + final_data);
			if (final_data.split(",")[1].equals("failed")) {
				updateScheduleTableStatus = "false";
			} else if (final_data.split(",")[1].equals("success_0")) {
				updateScheduleTableStatus = "true_0";
			} else {
				updateScheduleTableStatus = "true";
			}
			try {
				updateScheduleTableTransaction(updateScheduleTableStatus, this.tableNameSchedule,
						final_data.split(",")[2], final_data.split(",")[0], final_data.split(",")[3], this.db_URL,
						this.db_User, this.db_Pass, this.inputDate);
			} catch (Exception e) {
				e.printStackTrace();
			}
			String eventDate = (lastProcessedDate == null || lastProcessedDate.trim().isEmpty()) ? this.inputDate
					: lastProcessedDate;
			ev.insertEpEvents(eventDate, updateScheduleTableStatus, this.eventTable, final_data.split(",")[0],
					final_data.split(",")[4], final_data.split(",")[5], final_data.split(",")[6],
					final_data.split(",")[7], final_data.split(",")[2], final_data.split(",")[3],
					final_data.split(",")[8], final_data.split(",")[9], "event_end", db_URL, db_User, db_Pass);
		} catch (Exception eee) {
			System.out.println("Exception while processing in updateStatusAndEvent function");
			eee.printStackTrace();
		}
	}

	private void updateScheduleTableTransaction(String status, String scheduleTableName, String transactionTableName,
			String threadName, String sno, String db_URL, String db_User, String db_Pass, String inputDate) {
		Connection conn = null;
		PreparedStatement ps = null;
		String query = null;
		String last_export_time = "";
		try {
			String sync_status = "";
			if (status.equalsIgnoreCase("true")) {
				sync_status = "Ready_for_Export_Fre";
			} else if (status.equalsIgnoreCase("true_0")) {
				sync_status = "Ready_for_Export_Fre_0";
			} else {
				sync_status = "Error_EP_Fre";
			}
			query = "update " + scheduleTableName
					+ " set last_export_timestamp=?, sync_status=? where sno=? and table_name=?";
			conn = new com.fapps.db.MySQLConnection().getConnection(db_URL, db_User, db_Pass);
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(query);
			last_export_time = inputDate + " " + currentTime().split(" ")[1];
			ps.setString(1, last_export_time);
			ps.setString(2, sync_status);
			ps.setString(3, sno);
			ps.setString(4, transactionTableName);
			ps.addBatch();
			pu.sop("[" + threadName + "] Insert query after batch addition", ps.toString());
			ps.executeBatch();
			conn.commit();
		} catch (Exception ex) {
			System.out.println("[" + threadName + "] EXCEPTION JAR: updateTable: [" + threadName + "]" + ex.toString());
			ex.printStackTrace();
		} finally {
			try {
				if (conn != null) {
					if (ps != null)
						ps.close();
					conn.close();
				}
			} catch (Exception e) {
			}
		}
	}
}