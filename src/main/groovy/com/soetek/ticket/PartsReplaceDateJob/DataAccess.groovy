package com.soetek.ticket.PartsReplaceDateJob

import groovy.json.JsonOutput
import groovy.sql.Sql

import javax.naming.Context
import javax.naming.InitialContext
import javax.sql.DataSource
import java.sql.Connection

class DataAccess {

    static def process(Connection dbConn) {

        try {
            if (dbConn != null) {
                def gSQL = new Sql(dbConn)
                def sql = ""

                sql = "  SELECT C.[CustomerName], B.[SAP_DN_Number], C.[Email], (SELECT TOP 1 [ParameterValue] AS 'Url' FROM [dbo].[Parameter] WITH(NOLOCK) WHERE [ParameterGroupCode] = 'URL' AND [ParameterCode] = 'NotConfirmed') + CONVERT(NVARCHAR, B.[Id]) AS 'URL',   "
                sql += "(SELECT TOP 1 [ParameterValue] AS 'Day' "
                sql += "FROM [dbo].[Parameter] WITH(NOLOCK) WHERE [ParameterGroupCode] = 'Notice' AND [ParameterCode] = 'NotConfirmed')  AS 'Day' "
                sql += " FROM  [dbo].[Order] B WITH(NOLOCK) "
                sql += " JOIN [dbo].[Customer] C WITH(NOLOCK) ON B.[CustomerNo] = C.[CustomerNo]"
                sql += " JOIN [dbo].[CustomerType] D WITH(NOLOCK) ON C.[CustomerTypeCode] = D.[CustomerTypeCode] AND D.[CustomerTypeCode] <> '02' "
                sql += " WHERE DATEDIFF(DAY, CONVERT(DATE, B.[ActualShippingDate]), CONVERT(DATE,SYSDATETIME())) = "
                sql += "   (SELECT TOP 1 [ParameterValue] AS 'Day'  "
                sql += " FROM [dbo].[Parameter] WITH(NOLOCK) WHERE [ParameterGroupCode] = 'Notice' AND [ParameterCode] = 'NotConfirmed') "
                sql += " AND B.[OrderStatusCode] = '70' AND ISNULL(C.[Email], '') <> ''   "


                def data = []
                gSQL.eachRow(sql) { row ->
                    data << [
                            CustomerName : "${row.CustomerName}",
                            SAP_DN_Number: "${row.SAP_DN_Number}",
                            Email        : "${row.Email}",
                            URL          : "${row.URL}",
                            Day          : "${row.Day}"
                    ]
                }

                if (data.size() <= 0) {
                    return
                }

                def subject = "訂單到貨確認提醒：" + data[0].SAP_DN_Number.toString()
                def content = '''
%s您好，
您的訂單已經到貨超過%s天，請至系統確認交貨單號： %s，並按下「到貨確認」，請參考以下網址。
%s
'''

                content = String.format(content, data[0].CustomerName.toString(), data[0].Day.toString(), data[0].SAP_DN_Number.toString(), data[0].URL.toString())

                sql = "INSERT INTO [dbo].[Email] ([Subject], [ContentData], [ToAddress]) "
                sql += "VALUES (?, ?, ?) "

                def param = [];
                param.add(subject)
                param.add(content)
                param.add(data[0].Email.toString())

                def id = gSQL.executeInsert(sql, param)
            } else {
                return
            }
        } catch (Exception err) {
            err.printStackTrace()
        }
    }
}
