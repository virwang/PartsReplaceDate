package com.soetek.ticket.PartsReplaceDateJob

import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import org.slf4j.Logger

import java.sql.Connection

class ConnectionProvider {
    protected String ConnectionUrl;
    protected Logger logger
    public ResultSetHelper rsHelper

    // 提供resultFields判斷基準
    protected Integer status
    protected String error = ""

    /**
     * 建構子
     * @param logger
     * @param ConnectionUrl
     */
    public ConnectionProvider(Logger logger, String jndiDB) {
        this.logger = logger
        this.ConnectionUrl = jndiDB
        this.rsHelper = new ResultSetHelper(this.logger, ConnectionUrl)
    }

    /**
     * 連線資料庫查詢資料
     * @param sqlString
     * @return
     */
    protected ArrayList<Map<String, Object>> GetDataList(GString sqlString) {
        Connection connection
        try {
            connection = rsHelper.DBConnect(ConnectionUrl, logger)
            if (connection != null) {
                def gSQL = new Sql(connection)
                List<GroovyRowResult> list = gSQL.rows(sqlString)
                this.status = 0
                return list
            } else {
                this.status = -98
                return (ArrayList<Map<String, Object>>) Collections.EMPTY_LIST
            }
        } catch (Exception err) {
            this.status = -99
            this.error = "系統錯誤: " + err.toString()
            logger.error(err.getLocalizedMessage())
            logger.error(err.toString())
            return (ArrayList<Map<String, Object>>) Collections.EMPTY_LIST
        } finally {
            if (connection != null) {
                connection.close()
            }
        }
    }


    /**
     * 連線資料庫單筆新增/修改/刪除資料
     * @param sqlString
     * @return
     */
    protected Boolean ExecuteData(GString sqlString) {
        Connection dbConn = null
        try {
            dbConn = rsHelper.DBConnect(ConnectionUrl, logger)
            if (dbConn != null) {
                def gSQl = new Sql(dbConn)
                gSQl.withTransaction {
                    gSQl.execute(sqlString)
                }
                this.status = 0
                return true
            } else {
                this.status = -98
                return false
            }
        } catch (Exception err) {
            this.status = -99
            this.error = "系統錯誤: " + err.toString()
            logger.error(err.getLocalizedMessage())
            logger.error(err.toString())
            return false
        } finally {
            if (dbConn != null) {
                dbConn.close()
            }
        }
    }


    /**
     * 連線資料庫多筆新增/修改/刪除資料
     * @param gsStringList
     * @return
     */
    protected Boolean ExecuteMultiData(List<GString> gsStringList) {
        Connection dbConn = null
        try {
            dbConn = rsHelper.DBConnect(ConnectionUrl, logger)
            if (dbConn != null) {
                def gSQL = new Sql(dbConn)
                gSQL.withTransaction {
                    for (GString gsString : gsStringList) {
                        gSQL.execute(gsString)
                    }
                }
                this.status = 0
                return true
            } else {
                this.status = -98
                return false
            }
        } catch (Exception err) {
            this.status = -99
            this.error = "系統錯誤: " + err.toString()
            logger.error(err.getLocalizedMessage())
            logger.error(err.toString())
            return false
        } finally {
            if (dbConn != null) {
                dbConn.close()
            }
        }
    }


}
