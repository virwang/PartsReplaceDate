package com.soetek.ticket.PartsReplaceDateJob;

import org.jetbrains.annotations.NotNull;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

//todo:試跑一次
@DisallowConcurrentExecution
public class TimelyBatch {
    private final Logger _logger = LoggerFactory.getLogger(TimelyBatch.class);
    private final String _jobGroup;
    private final String _jobName;
    private final String _arg0;
    private final String _connectionString;
    private final String user;

    /*
     constructor
    */
    public TimelyBatch(String jobGroup, String jobName, String arg0, String connectionString, String userId) {
        _jobGroup = jobGroup;
        _jobName = jobName;
        _arg0 = arg0;
        _connectionString = connectionString;
        this.user = userId;
    }

    private @NotNull
    JobDataMap setJobParameters() {
        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put("arg0", _arg0);
        jobDataMap.put("connectionString", _connectionString);
        jobDataMap.put("userId", user);

        return jobDataMap;
    }

    public Trigger setJobInterval(String cronString) {
        return TriggerBuilder.newTrigger()
                .withIdentity(_jobName, _jobGroup)
                .withSchedule(CronScheduleBuilder.cronSchedule(cronString).withMisfireHandlingInstructionFireAndProceed())
                .startNow()
                .build();
    }

    public void Run(Trigger jobTrigger, long waitMinutes) {
        if (_connectionString == null || _connectionString.isEmpty()) {
            System.out.println("抓不到連線字串");
            return;
        } else {
            System.out.println("connection=" + _connectionString);
        }

        try {
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.start();

            JobDetail jobDetail = JobBuilder.newJob(PartsReplaceDateJob.class).usingJobData(setJobParameters())
                    .withIdentity(_jobName, _jobGroup)
                    .build();

            _logger.info("排程已啟動...");
            scheduler.scheduleJob(jobDetail, jobTrigger);

            if (waitMinutes > 0) {
                TimeUnit.MINUTES.sleep(waitMinutes);
                scheduler.shutdown(true);
            }
        } catch (SchedulerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void RunOnce() {
        if (_connectionString == null || _connectionString.isEmpty()) {
            System.out.println("抓不到連線字串");
            return;
        } else {
            System.out.println("connection=" + _connectionString);
        }

        try {
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.start();

            Trigger runOnceTrigger = TriggerBuilder
                    .newTrigger()
                    .withIdentity(_jobName, _jobGroup)
                    .startNow()
                    .build();

            JobDetail jobDetail = JobBuilder.newJob(PartsReplaceDateJob.class).usingJobData(setJobParameters())
                    .withIdentity(_jobName, _jobGroup)
                    .build();

            scheduler.scheduleJob(jobDetail, runOnceTrigger);
            _logger.info("單次執行");
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("參數錯誤");
            return;
        }

        String jobGroup = "TimelyBatch";
        String jobName = "PartsReplaceDateJob";
        TimelyBatch worker = null;
        String user = "AUT0001";

        //設定系統參數
        String connectionString = "";

        switch (args[0]) {
            case "PRD":
                connectionString = "";
                worker = new TimelyBatch(jobGroup, jobName, args[0], connectionString, user);
                worker.Run(worker.setJobInterval("0 0/1 * * * ?"), -1); //每1分鐘一次
                break;
            case "QAS":
                connectionString = "jdbc:mysql://192.168.21.53:3306/demodb370?user=michael&password=1qaz&useUnicode=true&characterEncoding=utf8";
                worker = new TimelyBatch(jobGroup, jobName, args[0], connectionString, user);
                worker.Run(worker.setJobInterval("0/15 * * * * ?"), -1);
                break;
            case "immediately":
                connectionString = "jdbc:mysql://192.168.21.53:3306/demodb370?user=michael&password=1qaz&useUnicode=true&characterEncoding=utf8";
                worker = new TimelyBatch(jobGroup, jobName, args[0], connectionString, user);
                worker.RunOnce();
                break;
            default:
                System.out.println("參數錯誤");
                return;
        }
    }
}
