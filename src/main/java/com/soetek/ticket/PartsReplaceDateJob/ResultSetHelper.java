package com.soetek.ticket.PartsReplaceDateJob;

import org.jetbrains.annotations.NotNull;
import org.quartz.CronExpression;
import org.slf4j.Logger;

import javax.sql.DataSource;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ResultSetHelper {
    protected Logger logger;
    public DataSource dbSource;


    public ResultSetHelper(Logger logger, String jndiDB) throws SQLException {
        this.logger = logger;
//        GetConnection(jndiDB);
//        ConnectDB(jndiDB);
    }


    //sql連線字串
//    protected void GetConnection(String jndiDB) throws SQLException {
//        try {
//            Context webContainer = new InitialContext();
//            this.dbSource = (DataSource) webContainer.lookup(jndiDB);
//        } catch (NamingException e) {
//            logger.error(e.getLocalizedMessage());
//        }
//    }
    //sql連線字串
//    protected void ConnectDB(String ConnectionString) throws SQLException {
//        Boolean connectCussess = true;
//        try {
//            Connection connection = DriverManager.getConnection(ConnectionString);
//            logger.info("資料庫連線成功");
//
//            return;
//        } catch (Exception e) {
//            logger.error(e.getLocalizedMessage());
//        }
//
//    }

    //sql指令: 抓取 jobTask
    public static final String sql_JobTask =
            "SELECT Id, IsAuto, FuncCode, ExpectedTime, FunStatus, GETDATE() as [GETDATE] FROM dbo.JobTask WITH (NOLOCK) \n" +
                    "WHERE FuncCode = 'T010A1' And ExpectedTime < GETDATE() AND FunStatus = 0 order by Id";

    //sql指令: 更新 jobTask 為執行中
    public static final String sql_UpdateJobTaskToExecuteStatus =
            "UPDATE [dbo].[JobTask] SET [FunStatus] = 1 WHERE Id IN ( ? )";

    //sql指令: 更新 jobTask 為執行結束
    public static final String sql_UpdateJobTaskToFinishStatus =
            "UPDATE [dbo].[JobTask]\n" +
                    "   SET [ActualStartTime] = ? \n" +
                    "      ,[ActualEndTime] = ? \n" +
                    "      ,[OutputContent] = ? \n" +
                    "      ,[ExceptionContent] = ? \n" +
                    "      ,[FunStatus] = 2 \n" +
                    "      ,[UpdatedTime] = SYSDATETIME() \n" +
                    "      ,[UpdatedBy] = 'User1' \n" +
                    " WHERE [Id] = ? ";

    //sql指令: 新增jobtask
    public static final String sql_InsertJobTask =
            "INSERT INTO [dbo].[JobTask] \n" +
                    "           ([FuncCode] \n" +
                    "           ,[ExpectedTime] \n" +
                    "           ,[InputContent] \n" +
                    "           ,[FunStatus] \n" +
                    "           ,[IsAuto] \n" +
                    "           ,[CreatedTime] \n" +
                    "           ,[CreatedBy]) \n" +
                    "     VALUES\n" +
                    "           ('T010A1'\n" +
                    "           , ? \n" +
                    "           ,''\n" +
                    "           ,0\n" +
                    "           ,1\n" +
                    "           ,SYSDATETIME()\n" +
                    "           ,'User1')";

    //sql指令: 查詢parameter
    public static final String sql_Parameter =
            "SELECT Id, ParameterGroupCode, ParameterCode, ParameterName, ParameterValue\n" +
                    "  FROM dbo.[Parameter]\n" +
                    "  where ParameterGroupCode = 'Batch'\n" +
                    "    and ParameterCode = 'T010'\n" +
                    "    and DeleteFlag = 0";

    public static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    //ResultSet 轉為 Map
    public static ArrayList toMap(@NotNull ResultSet rs) throws SQLException {
        ArrayList<Map<String, Object>> out = new ArrayList<>();

        while (rs.next()) {
            Map<String, Object> row = new HashMap<>();
            int numOfColumns = rs.getMetaData().getColumnCount();
            for (int i = 0; i < numOfColumns; i++) {
                row.put(rs.getMetaData().getColumnLabel(i + 1), rs.getObject(i + 1));
            }
            out.add(row);
        }
        return out;
    }

    //由Map萃取所有的Id並串接為字串
    public static String buildInString(ArrayList<Map<String, Object>> mapList, String idColumnName) {
        //Id欄位名稱的預設值
        if (idColumnName == null || idColumnName.isEmpty()) {
            idColumnName = "Id";
        }

        //串接Id
        StringBuilder Ids = new StringBuilder();
        for (Map<String, Object> map : mapList) {
            Ids.append(map.get(idColumnName).toString()).append(",");
        }
        Ids.deleteCharAt(Ids.length() - 1);
        return Ids.toString();
    }


    //更新 jobTask 為執行結束
    public static int UpdateJobTaskToFinishStatus(
            Connection connection, Map<String, Object> jobInExecStatus,
            LocalDateTime ActualStartTime, String OutputContent, String ExceptionContent, Logger _logger) throws SQLException {

        try (PreparedStatement p_updateJobTaskToFinishStatus = connection.prepareStatement(sql_UpdateJobTaskToFinishStatus)) {
            p_updateJobTaskToFinishStatus.setString(1, ActualStartTime.format(formatter));
            p_updateJobTaskToFinishStatus.setString(2, LocalDateTime.now().format(formatter));
            p_updateJobTaskToFinishStatus.setString(3, OutputContent);
            p_updateJobTaskToFinishStatus.setString(4, ExceptionContent);
            p_updateJobTaskToFinishStatus.setString(5, jobInExecStatus.get("Id").toString());
            return p_updateJobTaskToFinishStatus.executeUpdate();
        }
    }

    public static int UpdateSimpleSqlCommand(Connection connection, Logger _logger, String sql_command) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql_command)) {
            return statement.executeUpdate();
        }
    }

    //抓取 jobTask
    public static ArrayList<Map<String, Object>> SelectJobTask(Connection connection, Logger _logger) {
        return SelectSimpleSqlCommand(connection, _logger, sql_JobTask);
    }

    //抓取CRON設定值
    private static ArrayList<Map<String, Object>> SelectParameter(Connection connection, Logger _logger) {
        return SelectSimpleSqlCommand(connection, _logger, sql_Parameter);
    }

    //執行不需要替換參數的sql指令，並以ArrayList<Map<String, Object>>格式回傳結果
    @NotNull
    public static ArrayList<Map<String, Object>> SelectSimpleSqlCommand(Connection connection, Logger _logger, String sql_command) {
        ArrayList<Map<String, Object>> resultMapList;

        try (PreparedStatement statement = connection.prepareStatement(sql_command);
             ResultSet rs = statement.executeQuery()) {
            resultMapList = ResultSetHelper.toMap(rs);
            return resultMapList;
        } catch (SQLException ex) {
            _logger.error("[PartsReplaceDateJob]" + ex.toString());
            return new ArrayList();
        }
    }

    public static int InsertJobTask(Connection connection, Logger _logger, Map<String, Object> lastAutoJobTask) throws SQLException {
        ArrayList<Map<String, Object>> CronParameters = SelectParameter(connection, _logger);

        if (CronParameters.size() == 0) {
            return 0;
        }

        Map<String, Object> CronParameter = CronParameters.get(0);

        CronExpression cron;
        try {
            cron = new CronExpression(CronParameter.get("ParameterValue").toString());
        } catch (ParseException e) {
            _logger.error("[PartsReplaceDateJob]新增jobTask失敗");
            _logger.error(e.toString());
            return 0;
        }

        java.util.Date nextValidTime = cron.getNextValidTimeAfter(new java.util.Date());
        String expected = sdf.format(nextValidTime);

        try (PreparedStatement statement = connection.prepareStatement(sql_InsertJobTask)) {
            statement.setString(1, expected);
            return statement.executeUpdate();
        }
    }

    public static Connection DBConnect(String ConnectUrl, Logger logger) {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(ConnectUrl);
            logger.info("連線成功");
        } catch (Exception e) {

            logger.error(e.getLocalizedMessage());

        }
        return connection;
    }
}
