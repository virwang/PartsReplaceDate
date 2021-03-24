package com.soetek.ticket.PartsReplaceDateJob;

import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;

@DisallowConcurrentExecution
public class PartsReplaceDateJob implements Job {
    private final Logger _logger = LoggerFactory.getLogger(PartsReplaceDateJob.class);

    //由 main 傳過來的參數
    protected String _arg0;
    protected String _connectionString;
    protected String user;

    //helper
    protected LogicProvider logic;

    //Log字首
    public static final String logPrefix = "[PartsReplaceDateJob]";

    //資料庫抓取的資料
    protected ArrayList<Map<String, Object>> jobTasks = new ArrayList<>();
    protected Boolean JobSuccess;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        //清除參數
        _arg0 = "";
        _connectionString = "";

        //取得參數
        JobDetail jobDetail = context.getJobDetail();
        if (jobDetail == null) {
            _logger.info(logPrefix + "抓不到參數(all)");
            return;
        }

        _arg0 = jobDetail.getJobDataMap().getString("arg0");
        _connectionString = jobDetail.getJobDataMap().getString("connectionString");
        user = jobDetail.getJobDataMap().getString("userId");
        _logger.info(logPrefix + "arg0:" + _arg0);
        _logger.info(logPrefix + "connectionString:" + _connectionString);
        _logger.info(logPrefix + " user :" + user);

        //紀錄排程開始的 log
        if (context.getFireTime() != null) {
            _logger.info(logPrefix + "觸發: " + ResultSetHelper.sdf.format(context.getFireTime()));
        } else {
            _logger.info(logPrefix + "觸發: 抓不到觸發時間");
        }

        //Provider建立
        logic = new LogicProvider(_connectionString, ResultSetHelper.sdf.format(context.getFireTime()), user);

        //主邏輯
        //只要排程啟動，就檢查今天是否已經有排程工作，若無->不執行作業
        //排程分類 & 各種邏輯在logic內執行
        jobTasks = logic.SelectJobTask();
        if (logic.status != 0) {
            WriteIntoLogger();
            return;
        }

        logic.CheckReturn(jobTasks);
        if (logic.status != 0) {
            WriteIntoLogger();
            return;
        }

        // 成功 or 失敗-> 更新JobTask
        JobSuccess = logic.UpdateJobTask();
        if (logic.status != 0 || !JobSuccess) {
            WriteIntoLogger();
            return;
        }

        JobSuccess = logic.CreateTomorrowJobTask();
        if (logic.status != 0 || !JobSuccess) {
            WriteIntoLogger();
        }
    }

    //檢查新增或更新狀態(成功或失敗)
    private void WriteIntoLogger() {
        if (logic.status == -99) {
            _logger.error(logPrefix + "排成已啟動，但資料庫連線失敗狀態代碼: " + logic.status + " 錯誤訊息:" + logic.error);
        }
        if (logic.status == -94) {
            _logger.info(logPrefix + "排程已啟動，查無更新或者新增的資料，進行日常排程作業");
        }

        if (logic.status < 0) {
            _logger.error(logPrefix + "排程已啟動，但資料新增或更新失敗，狀態代碼: " + logic.status + " 錯誤訊息:" + logic.error);
        }
    }

}
