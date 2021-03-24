package com.soetek.ticket.PartsReplaceDateJob;

import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class LogicProvider {

    //from TimelyBatch
    protected String Connection;
    //logger
    protected Logger logger;
    //BatchActualStartTime
    protected String FireDateTime;
    //userId
    protected String userId;

    //for judgement
    protected Integer status = 0;
    protected String error = "";
    ArrayList<Map<String, Object>> Check = new ArrayList<>();
    Map<String, Object> CheckEntry = new HashMap<>();

    //SQL Provider
    public TicketProvider provider;
    //Convert Helper
    public SimpleConvertHelper Converter = new SimpleConvertHelper();

    //params for BatchJob
    protected LocalDate YesterdayDate = LocalDate.now().minusDays(1);
    protected LocalDateTime DateTimeNow = LocalDateTime.now();
    //for排程程式
    protected LocalDateTime StartOfToday = LocalDate.now().atStartOfDay();
    protected LocalDateTime TomorrowExpBatchStart = LocalDate.now().plusDays(1).atTime(6, 0, 0);
    protected LocalDateTime BatchExpStart = LocalDate.now().atTime(6, 0, 0);
    protected LocalDateTime yesterday = YesterdayDate.atStartOfDay();

    //轉型
    protected String Now = Converter.LocalDateTimeToString(DateTimeNow);
    protected String YesterdayStart = Converter.LocalDateTimeToString(yesterday);
    protected String Tomorrow = Converter.LocalDateTimeToString(TomorrowExpBatchStart);
    protected String StartToday = Converter.LocalDateTimeToString(StartOfToday);
    protected String BatchExpStartToday = Converter.LocalDateTimeToString(BatchExpStart);

    //中繼資料
    protected ArrayList<InputModelHelper> InputList = new ArrayList<>();
    protected ArrayList<Map<String, Object>> result = new ArrayList<>();
    protected ArrayList<Map<String, Object>> ForUpdateLogic = new ArrayList<>();
    protected ArrayList<Map<String, Object>> ForCreateLogic = new ArrayList<>();
    protected ArrayList<Map<String, Object>> ForRoutineLogic = new ArrayList<>();
    protected ArrayList<Map<String, Object>> TempArrayListMap = new ArrayList<>();
    protected ArrayList<Map<String, Object>> TempArray = new ArrayList<>();
    protected ArrayList<Map<String, Object>> ChangeMaterialResult = new ArrayList<>();
    protected ArrayList<Map<String, Object>> ChangeMaterialsFromTicket = new ArrayList<>();
    protected ArrayList<Map<String, Object>> MaterialCycleData = new ArrayList<>();
    protected ArrayList<Map<String, Object>> SerialTicketData = new ArrayList<>();
    protected ArrayList<Map<String, Object>> ChangeDateTicketData = new ArrayList<>();
    protected ArrayList<Map<String, Object>> JobTaskList = new ArrayList<>();
    protected Map<String, Object> TempMap = new HashMap<>();
    protected Map<String, Object> JobTaskMap = new HashMap<>();
    protected Map<List<Integer>, Object> MaterialTicketMap = new HashMap<>();
    protected Map<List<Integer>, Object> MaterialCycleMap = new HashMap<>();
    protected Map<List<Integer>, Object> SerialNoMap = new HashMap<>();
    protected Map<List<Integer>, Object> ChangeDateMap = new HashMap<>();
    protected Map<List<Integer>, Object> MaterialCycle = new HashMap<>();


    //資料庫回傳的資料
    protected List<String> TempStringList = new ArrayList<>();
    protected List<Integer> TempIntList = new ArrayList<>();
    protected List<Integer> TicketId = new ArrayList<>();
    protected List<Integer> MaterialId = new ArrayList<>();
    protected List<Integer> MaterialByTicket = new ArrayList<>();
    protected List<Integer> MaterialAll = new ArrayList<>();
    protected List<Integer> MaterialStatisticA = new ArrayList<>();
    protected List<Integer> MaterialStatisticB = new ArrayList<>();
    protected List<Integer> MaterialStatisticC = new ArrayList<>();
    protected List<Integer> ForDBReturn = new ArrayList<>();
    protected List<Integer> JobTaskId = new ArrayList<>();
    protected List<Integer> MachineTicketList = new ArrayList<>();
    protected List<Integer> SerialTicket = new ArrayList<>();
    protected List<Integer> ForTemp = new ArrayList<>();
    protected List<Integer> ChangeCycle = new ArrayList<>();
    protected List<String> ChangedDate = new ArrayList<>();
    protected List<String> SerialNo = new ArrayList<>();
    protected List<String> MKeyNo = new ArrayList<>();
    protected List<String> Material = new ArrayList<>();
    protected List<Boolean> DeleteFlag = new ArrayList<>();


    //for sql create & update
    protected Boolean IsSuccess = true;

    /**
     * 建構子
     */
    public LogicProvider(@NotNull String connection, String fireDateTime, String User) {
        this.logger = LoggerFactory.getLogger(LogicProvider.class);
        this.Connection = connection;
        this.userId = User;
        this.FireDateTime = fireDateTime;
        this.provider = new TicketProvider(logger, connection);
    }


    /**
     * 符合條件的ticket
     * <p>
     * 參數
     * LocalDateTime startTime;(排程開始時間)
     * 1.今天&昨天新增或者異動的單子
     * 2.僅需要TicketId
     */
    void SelectTicket() {
        if (result != null && !result.isEmpty()) {
            result.clear();
        }

        //清空暫存
        TempArrayListMap.clear();

        //create
        this.result = provider.SelectTicket(YesterdayStart, true, false);
        CheckStatus(provider.status);

        if (this.status != 0) {
            return;
        }

        //update
        this.TempArrayListMap = provider.SelectTicket(YesterdayStart, false, true);
        CheckStatus(provider.status);

        if (this.status != 0) {
            return;
        }

        //如果查詢結果都是0，直接return
        if (TempArrayListMap.size() == 0 && result.size() == 0) {
            return;
        }

        //清空佔存
        ForDBReturn.clear();
        ForTemp.clear();

        ArrayList<Integer> TicketTempList = new ArrayList<>();

        //新增的Ticket查詢結果
        if (result.size() > 0) {
            int TicketKey = 0;
            List<String> tickets = new ArrayList<>();

            for (Map<String, Object> stringObjectMap : result) {
                String ticket = stringObjectMap.get("TicketId").toString();

                tickets.add(ticket);
                String ticketIds;
                for (String TicketString : tickets) {
                    ticketIds = TicketString;
                    TicketKey = Integer.parseInt(ticketIds);
                }
                ForTemp.add(TicketKey);
            }
            TicketTempList = new ArrayList<>(ForTemp);
        }


        //更新的Ticket查詢結果
        List<Integer> TempUpdateTicket = new ArrayList<>();
        int TicketKey = 0;
        if (TempArrayListMap.size() > 0) {
            for (Map<String, Object> stringObjectMap : TempArrayListMap) {
                String ticket = stringObjectMap.get("TicketId").toString();
                TicketKey = Integer.parseInt(ticket);
            }
            TempUpdateTicket.add(TicketKey);
            TicketTempList.addAll(TempUpdateTicket);
        }
        this.TicketId = TicketTempList.stream().distinct().collect(Collectors.toList());
    }

    /**
     * 今天是否有新增
     * 時間範圍: 昨天至今天;
     * 需要: MaterialId、MKeyNo、ChangeCycle、DeleteFlag
     */
    //TODO:測試資料是否有抓取到正確的值
    void SelectCreatedMaterialCycle() {
        if (result != null && !result.isEmpty()) {
            result.clear();
        }

        this.result = provider.SelectMaterialCycle(MaterialId, true, false, YesterdayStart);
        CheckStatus(provider.status);

        if (this.status != 0) {
            return;
        }

        if (result.size() == 0) {
            return;
        }

        //清空暫存資料
        TempMap.clear();
        TempArrayListMap.clear();
        MaterialId.clear();
        MKeyNo.clear();
        ChangeCycle.clear();
        DeleteFlag.clear();

        int id = 0;
        String MKey = "";
        String Change = "";
        int cycle = 0;
        String Flag = "";
        boolean IsDelete = false;

        for (Map<String, Object> stringObjectMap : result) {
            for (Map.Entry<String, Object> entry : stringObjectMap.entrySet()) {
                String Key = entry.getKey();
                String Value = entry.getValue().toString();

                if (Key.equals("MaterialId")) {
                    id = Integer.parseInt(Value);
                }

                if (Key.equals("MKeyNo")) {
                    MKey = Value;
                }

                if (Key.equals("ChangeCycle")) {
                    Change = Value;
                    cycle = Integer.parseInt(Change);
                }

                if (Key.equals("DeleteFlag")) {
                    if (Value == null || Value.isEmpty()) {
                        Value = "";
                    }
                    Flag = Value;

                    IsDelete = !Flag.isEmpty();
                }

                TempMap.put("MaterialId", id);
                TempMap.put("MKeyNo", MKey);
                TempMap.put("ChangeCycle", Change);
                TempMap.put("DeleteFlag", Flag);
            }
            //新增的MaterialCycle資料，塞入暫存ArrayList<Map<String,Object>

            this.MaterialId.add(id);
            this.MKeyNo.add(MKey);
            this.ChangeCycle.add(cycle);
            this.DeleteFlag.add(IsDelete);
        }
    }

    /**
     * 今天是否有更新
     * 時間範圍: 昨天至今天
     * 需要: MaterialId,MKeyNo,ChangeCycle,DeleteFlag
     */
    void SelectUpdatedMaterialCycle() {
        if (result != null && !result.isEmpty()) {
            result.clear();
        }

        this.result = provider.SelectMaterialCycle(MaterialId, false, true, YesterdayStart);

        CheckStatus(provider.status);
        if (this.status != 0) {
            return;
        }

        CheckReturn(this.result);
        if (this.status != 0) {
            return;
        }

        int Id = 0;
        String MKeys = "";
        int cycle = 0;
        boolean flag = false;
        TempMap.clear();
        for (Map<String, Object> stringObjectMap : result) {
            String Key = "";
            String Value = "";

            //抓取需要的欄位
            for (Map.Entry<String, Object> entry : stringObjectMap.entrySet()) {
                Key = entry.getKey();
                Value = entry.getValue().toString();

                if (Key.equals("MaterialID")) {
                    Id = Integer.parseInt(Value);
                }

                if (Key.equals("MKeyNo")) {
                    MKeys = Value;
                }

                if (Key.equals("changeCycle")) {
                    cycle = Integer.parseInt(Value);
                }

                if (Key.equals("DeleteFlag")) {
                    flag = Value != null && !Value.isEmpty();

                }
                TempMap.put("MaterialId", Id);
                TempMap.put("MKeyNo", MKeys);
                TempMap.put("ChangeCycle", cycle);
                TempMap.put("DeleteFlag", flag);
            }
            this.MaterialId.clear();
            this.ChangeCycle.clear();
            this.DeleteFlag.clear();

            this.MaterialId.add(Id);
            this.ChangeCycle.add(cycle);
            this.DeleteFlag.add(flag);

            this.ForUpdateLogic.add(TempMap);
        }

    }

//    void GetMachineType() {
//        if (result != null && !result.isEmpty()) {
//            result.clear();
//        }
//
//        String label = "機台型號";
//        TempStringList.clear();
//        this.result = provider.SelectFormEntryValueByTicket(label, TicketId, this.FireDateTime, false, false, false, TempStringList);
//
//        CheckStatus(provider.status);
//        if (this.status != 0) {
//            return;
//        }
//
//        int MachineKey;
//        if (result.size() > 0) {
//            for (Map<String, Object> stringObjectMap : result) {
//                String machine = stringObjectMap.get("value").toString();
//                machine = machine.substring(0, machine.indexOf(":"));
//                machine = machine.substring(2, machine.length() - 1);
//                MachineKey = Integer.parseInt(machine);
//                this.MachineTypeId.add(MachineKey);
//            }
//        }
//    }

    /**
     * 由機台型號id -> listItem ->對應的機台名稱
     */
//    void GetMachineTypeName() {
//        if (result != null && !result.isEmpty()) {
//            result.clear();
//        }
//
//        String MachineTypeLabel = "機台型號";
//        this.result = provider.SelectList(MachineTypeId, MachineTypeLabel);
//
//        if (result.size() == 0) {
//            return;
//        }
//
//        String ListItem = "";
//        for (Map<String, Object> stringObjectMap : result) {
//            ListItem = stringObjectMap.get("listItem").toString();
//        }
//        this.MachineType.add(ListItem);
//    }

    /**
     * 抓取料件id
     * 在ticket欄位抓取料件id
     * 這裡只抓取更換料件
     */
//    void GetChangedMaterial() {
//        if (result != null && !result.isEmpty()) {
//            result.clear();
//        }
//        String label = "更換料件";
//        String cycle = "";
//        for (Integer CycleFloat : ChangeCycle) {
//            cycle = CycleFloat.toString();
//        }
//        TempStringList.add(cycle);
//
//        this.result = provider.SelectFormEntryValueByTicket(label, TicketId, YesterdayStart, false, false, false, TempStringList);
//        CheckStatus(provider.status);
//        if (this.status != 0) {
//            return;
//        }
//
//        if (result.size() == 0) {
//            return;
//        }
//
//        int MaterialKeys = 0;
//        int ticketKey;
//        String AllMaterialString = "";
//        for (Map<String, Object> stringObjectMap : result) {
//            String MaterialKey = stringObjectMap.get("Value").toString();
//            String Name = stringObjectMap.get("ticket_Id").toString();
//
//            ticketKey = Integer.parseInt(Name);
//            JSONObject MaterialNo = new JSONObject(MaterialKey);
//            Iterator<String> MaterialAllKey = MaterialNo.keys();
//
//            while (MaterialAllKey.hasNext()) {
//                AllMaterialString = MaterialAllKey.next();
//                MaterialKeys = Integer.parseInt(AllMaterialString);
//            }
//
//            this.MaterialAll.clear();
//            this.MaterialAll.add(MaterialKeys);
//            this.TicketId.clear();
//            this.TicketId.add(ticketKey);
//        }
//    }

    /**
     * 案件裡對應到的料件是否存在料件更換主檔?
     * 否->排除
     * 是->留下
     * 1.更換週期
     * 2. 料件id、名稱
     * 3. MKeyNo
     * 刪除旗標
     * 檢查料件id是否存在週期主檔中
     * 存在:
     * 一、MkeyNo
     * 二、ChangeCycle
     * 三、MaterialId
     */
    void SelectMaterialCycle() {
        if (result != null && !result.isEmpty()) {
            result.clear();
        }

        this.result = provider.SelectMaterialCycle(this.MaterialByTicket, false, false, BatchExpStartToday);
        CheckStatus(provider.status);
        if (this.status != 0) {
            return;
        }

        if (result.size() == 0) {
            return;
        }

        int MaterialKeys = 0;
        String MKey;
        String Cycle;
        int Change = 0;

        //清除資料庫資料
        Material.clear();
        MaterialId.clear();
        TempMap.clear();

        for (Map<String, Object> stringObjectMap : result) {
            String MaterialKey = stringObjectMap.get("MaterialId").toString();
            MKey = stringObjectMap.get("MKeyNo").toString();
            Cycle = stringObjectMap.get("ChangeCycle").toString();
            if (Cycle.contains(".")) {
                Cycle = Cycle.substring(0, Cycle.length() - 2);

                //轉型
                Change = Integer.parseInt(Cycle);
                MaterialKeys = Integer.parseInt(MaterialKey);
            }

            ///塞入list
            this.MKeyNo.add(MKey);
            this.ChangeCycle.add(Change);
            this.MaterialId.add(MaterialKeys);

        }

    }

    /**
     * 抓取表單欄位的內容
     * 1. 統計型更換料件A、B、C
     * 2.更換料件
     * 並且 -> 這邊要先更新一次ticketId
     * 用更新後的ticketId再去搜尋如下的對應資料
     * 3.更換時間日期
     * 4. SerialNo
     */
    void SelectChangeMaterialsFromTicket() {
        if (result != null && !result.isEmpty()) {
            result.clear();
        }

        //更換料件、統計型A、B、C
        int AllMaterial = 0;
        int MaterialA = 0;
        int MaterialB = 0;
        int MaterialC = 0;

        //用於抓取此條件的篩選出來的單號id
        int MachineTicketInt;

        //更換料件
        this.TempStringList.clear();
        this.ChangeMaterialResult.clear();
        String ChangeMaterialLabel = "更換料件";
        this.ChangeMaterialResult = provider.SelectFormEntryValueByTicket(ChangeMaterialLabel, TicketId, YesterdayStart, false, false, false, TempStringList);
        if (this.status != 0) {
            return;
        }

        String entry;
        //依照此條件下，篩選出來的ticketId
        String MaterialTicketString;
        MaterialAll.clear();
        TempMap.clear();
        for (Map<String, Object> objectMap : ChangeMaterialResult) {
            MachineTicketInt  = (int) objectMap.get("ticketId");
            entry = objectMap.get("value").toString();
            JSONObject jsonValue = new JSONObject(entry);
            Iterator<String> iterator = jsonValue.keys();

            while (iterator.hasNext()) {
                String MaterialValue = iterator.next();
                AllMaterial = Integer.parseInt(MaterialValue);

            }
            MachineTicketList = new ArrayList<>();
            MachineTicketList.add(MachineTicketInt);
        }

        //統計型A
        result.clear();
        String MaterialNameA = "統計型更換料件A";
        this.TempStringList.clear();

        this.result = provider.SelectFormEntryValueByTicket(MaterialNameA, this.TicketId, YesterdayStart, false, false, false, TempStringList);
        //檢查狀態
        CheckStatus(provider.status);
        if (this.status != 0) {
            return;
        }

        String NameA;
        for (Map<String, Object> objectMap : result) {
            MaterialTicketString = objectMap.get("ticketId").toString();
            MachineTicketInt = Integer.parseInt(MaterialTicketString);
            NameA = objectMap.get("value").toString();

            JSONObject MaterialIdA = new JSONObject(NameA);
            Iterator<String> iterator = MaterialIdA.keys();
            while (iterator.hasNext()) {
                String MA = iterator.next();
                MaterialA = Integer.parseInt(MA);
                //塞入list
                MaterialStatisticA = new ArrayList<>();
                this.MaterialStatisticA.add(MaterialA);
            }
            MachineTicketList.add(MachineTicketInt);
        }

        //統計型B
        String MaterialNameB = "統計型更換料件B";
        this.TempArrayListMap.clear();
        TempStringList.clear();

        this.TempArrayListMap = provider.SelectFormEntryValueByTicket(MaterialNameB, this.TicketId, YesterdayStart, false, false, false, TempStringList);
        String NameB;

        for (Map<String, Object> objectMap : result) {
            MaterialTicketString = objectMap.get("ticketId").toString();
            MachineTicketInt = Integer.parseInt(MaterialTicketString);
            NameB = objectMap.get("value").toString();

            JSONObject MB = new JSONObject(NameB);
            Iterator<String> iterator = MB.keys();

            while (iterator.hasNext()) {
                String MaterialIdB = iterator.next();
                MaterialB = Integer.parseInt(MaterialIdB);
                //塞進去list
                MaterialStatisticB = new ArrayList<>();
                this.MaterialStatisticB.add(MaterialB);
            }
            this.MachineTicketList.add(MachineTicketInt);
        }


        String MaterialNameC = "統計型更換料件C";
        this.TempStringList.clear();
        this.TempArray.clear();
        TempMap.clear();
        this.TempArray = provider.SelectFormEntryValueByTicket(MaterialNameC, this.TicketId, YesterdayStart, false, false, false, TempStringList);

        CheckStatus(provider.status);
        if (this.status != 0) {
            return;
        }

        if (this.TempArrayListMap.size() == 0 && this.result.size() == 0 && TempArray.size() == 0 && ChangeMaterialResult.size() == 0) {
            return;
        }

        String NameC;
        for (Map<String, Object> objectMap : TempArray) {
            MaterialTicketString = objectMap.get("ticketId").toString();
            MachineTicketInt = Integer.parseInt(MaterialTicketString);
            NameC = objectMap.get("value").toString();

            JSONObject MC = new JSONObject(NameC);
            Iterator<String> iterator = MC.keys();
            while (iterator.hasNext()) {
                MaterialStatisticC = new ArrayList<>();
                String MaterialIDC = iterator.next();
                MaterialC = Integer.parseInt(MaterialIDC);
                //所有更換料件都塞進去List & Map
                this.MaterialStatisticC.add(MaterialC);

            }
            MachineTicketList.add(MachineTicketInt);
        }

        //得到所有的料件id
        MaterialByTicket.clear();
        TempMap.clear();
        MaterialTicketMap = new HashMap<>();
        this.MaterialByTicket.addAll(MaterialAll);
        this.MaterialByTicket.addAll(MaterialStatisticA);
        this.MaterialByTicket.addAll(MaterialStatisticB);
        this.MaterialByTicket.addAll(MaterialStatisticC);
        this.MaterialByTicket = MaterialByTicket.stream().distinct().collect(Collectors.toList());

        //更新ticketId
        TicketId.clear();
        this.TicketId.addAll(MachineTicketList);
        this.TicketId = TicketId.stream().distinct().collect(Collectors.toList());
        ChangeMaterialsFromTicket = new ArrayList<>();
        MaterialTicketMap.put(TicketId, this.MaterialByTicket);
    }

    /**
     * 取得SerialNo
     */
    //todo:重複的情況 ，只保留最新一筆更換時間日期(新增功能)
    void SelectSerialNo() {
        if (result != null && !result.isEmpty()) {
            result.clear();
        }

        String SNO;
        String SerialNoLabel = "Serial No.";
//        int TicketOfSerialNo;
        TempStringList.clear();

        this.result = provider.SelectFormEntryValueByTicket(SerialNoLabel, this.TicketId, YesterdayStart, false, false, false, TempStringList);

        CheckStatus(provider.status);
        if (this.status != 0) {
            return;
        }

        if (result.size() == 0) {
            return;
        }
        int TicketOfSerialNo;
        TempMap.clear();
        for (Map<String, Object> objectMap : result) {
            SNO = objectMap.get("value").toString().trim();
            String ticketSerial = objectMap.get("ticketId").toString().trim();
            TicketOfSerialNo = Integer.parseInt(ticketSerial);

            this.SerialNo.add(SNO);
            this.SerialTicket.add(TicketOfSerialNo);
            SerialNoMap.put(SerialTicket, SerialNo);
//            SerialNoMap.put("SerialNo", SerialNo);
        }

    }

    /**
     * 取得更換時間日期欄位的資料
     */
    void SelectChangeDate() {
        if (result != null && !result.isEmpty()) {
            result.clear();
        }

        String ChangeDateLabel = "更換時間日期";
        this.TempStringList.clear();
        this.result = provider.SelectFormEntryValueByTicket(ChangeDateLabel, this.TicketId, YesterdayStart, false, false, false, TempStringList);
        CheckStatus(provider.status);
        if (this.status != 0) {
            return;
        }

        if (result.size() == 0) {
            return;
        }

        String Change;
        String ChangeTicketId;
        int id = 0;
        for (Map<String, Object> objectMap : result) {
            Change = objectMap.get("value").toString();
            ChangeTicketId = objectMap.get("ticketId").toString();
            Change = Change.substring(0, Change.length() - 3).trim();
            id = Integer.parseInt(ChangeTicketId);
            this.ChangedDate.add(Change);
        }

        this.TempIntList.add(id);

        ChangeDateMap.clear();
        ChangeDateMap.put(TempIntList, this.ChangedDate);
    }

    /**
     * 如果serialNo有重複，抓取最新的更換日期
     * 藉由TicketId來查找這筆資料
     * 回傳TicketId，然後再重新搜尋SerialNo
     */
    void GetLatestChangedDate() {
        if (result != null && !result.isEmpty()) {
            result.clear();
        }

        String ChangeDateLabel = "更換時間日期";
        this.result = provider.LatestChangeDate(this.SerialTicket, ChangeDateLabel);
    }

    /**
     * Goal:將資料塞入ArrayList<Map<String,Object>
     * 然後在sql資訊那邊for each
     * Table: InsertReplacePartsDate
     */
    void AddDataIntoArrayList() {
        int mid = 0;
        int ticketId = 0;
        String ChangeDate = "";
        int cycle = 0;
        String MK = "";
        LocalDate next;

    }


    /**
     * 塞入資料:
     * SerialNo
     * MKeyNo
     * PreviousDate
     * NextDate
     * Create = now()
     * CreatedBy
     */
    void InsertPartsReplaceDate() {
        if (!result.isEmpty()) {
            result.clear();
        }

        LocalDateTime NextDay;
        List<LocalDateTime> PredictDate = new ArrayList<>();
        String Next;
        int Modify;
        int ChangeWeek = 0;
        TempStringList.clear();

        //獲得下次更換日期
        if (this.ChangedDate.size() == 1) {
            NextDay = Converter.StringToLocalDateTime(ChangedDate.get(0));
            PredictDate.add(NextDay);
        }

        if (this.ChangedDate.size() > 1) {
            for (String s : this.ChangedDate) {
                NextDay = Converter.StringToLocalDateTime(s);
                PredictDate.add(NextDay);
            }
        }

        for (Integer cycle : ChangeCycle) {
            //變成天數
            ChangeWeek = cycle * 7;
        }

        ForTemp.clear();
        this.ForTemp.add(ChangeWeek);

        //轉型
        if (PredictDate.size() >= 1) {
            LocalDateTime DateTimeParse;
            for (LocalDateTime localDateTime : PredictDate) {
                for (Integer WeekDays : ForTemp) {
                    DateTimeParse = localDateTime.plusDays(WeekDays);
                    Next = Converter.LocalDateTimeToString(DateTimeParse);

                    TempStringList.clear();
                    TempStringList.add(Next);
                }
            }
        }
        this.IsSuccess = provider.InsertPartsReplaceDate(this.SerialNo, this.MKeyNo, this.ChangedDate, this.TicketId, TempStringList, userId);
    }


    /**
     * To check if JobTask exits
     * Success -> JobSeparation()
     * Fail -> return
     */
    ArrayList<Map<String, Object>> SelectJobTask() {
        if (result != null && !result.isEmpty()) {
            result.clear();
        }

        this.result = provider.SelectBatchJob(BatchExpStartToday, this.userId);
        CheckStatus(provider.status);

        if (this.status != 0) {
            return Check;
        }

        if (result.size() > 0) {
            String TaskId;
            String InParam;
            int ids = 0;

            for (Map<String, Object> stringObjectMap : result) {
                for (Map.Entry<String, Object> entry : stringObjectMap.entrySet()) {
                    TaskId = entry.getKey();
                    InParam = entry.getValue().toString();

                    if (TaskId.equals("Id")) {
                        ids = Integer.parseInt(InParam);
                    }
                    JobTaskMap.put(TaskId, InParam);
                    this.ForTemp.add(ids);
                }
                this.JobTaskList.add(JobTaskMap);
            }
            this.JobTaskId = ForTemp.stream().distinct().collect(Collectors.toList());
            //呼叫排程分類的method
            JobSeparation(JobTaskMap);
            return result;
        }

        return result;
    }

    /**
     * 進行排程任務的InParam欄位的 key 值 抓取並分類
     * 依照不同Key值進行不同邏輯的排程作業
     * KEY值:
     * 1.TICKET_ID:表示進行每日的排程作業 -> RoutineBatch()
     * 2.MATERIAL_ID:表示料件更換週期主檔有新增作業，執行 -> UpdateLogicBatch()
     * 3.MKey_No:表示料件更換週期主檔有更新作業，執行 -> CreateLogicBatch()
     */
    void JobSeparation(@NotNull Map<String, Object> Input) {
        for (Map.Entry<String, Object> entry : Input.entrySet()) {
            String InParam = entry.getKey();
            if (InParam.equals("InParam")) {
                String ParamKey = entry.getValue().toString();

                //轉為json資料型態，抓取key值
                JSONObject KeyObject = new JSONObject(ParamKey);
                Iterator<String> iterator = KeyObject.keys();

                while (iterator.hasNext()) {
                    String InPramKye = iterator.next();
                    switch (InPramKye) {
                        case "TICKET_ID":
                            RoutineBatch();
                            CreateTomorrowJobTask();
                            break;
                        case "MATERIAL_ID":
                            BatchCreatedLogic();
                            break;
                        case "MKeyNo":
                            BatchUpdatedLogic();
                            break;
                        default:
                            break;
                    }
                }
            }
        }
    }

    /**
     * 沒有新增或修改料件維護主檔
     * 進行日常排程作業
     * 1. 抓今天&昨天新增或編輯的ticket
     * 2. 機台型號存在 MaterialCycle? 有不存在的排除
     * 3. 更換料件&統計型 存在 MaterialCycle對應的型號料件? 不存在排除；需要型號&料件
     * 4. ticket的更換時間日期、SerialNo
     * 5. 抓與店家預計保養日期主檔對應到的型號 & 料件 ->需要 MKeyNo
     * 6. 將 SerialNo、MKeyNo 紀錄至機台料件更換主檔 PartsReplaceDate
     * 7. PartsReplaceDate紀錄資訊:
     * i. SerialNo -> ticket
     * ii. MKeyNo->料件更換週期主檔
     * iii. 上次更換日期-> ticket 更換時間日期
     * iv. 預計下次更換日期: 上次更換日期 +更換週期 * 7
     */
    //TODO:試運行一次，看看有沒有error
    protected void RoutineBatch() {
        SelectTicket();
        if (TempArrayListMap.size() == 0 && result.size() == 0) {
            return;
        }

        SelectChangeMaterialsFromTicket();
        if (TempArrayListMap.size() == 0 && result.size() == 0 && TempArray.size() == 0 && ChangeMaterialResult.size() == 0) {
            return;
        }

        SelectMaterialCycle();
        if (result.size() == 0) {
            return;
        }

        SelectSerialNo();
        if (result.size() == 0) {
            return;
        }

        SelectChangeDate();
        if (result.size() == 0) {
            return;
        }

        AddDataIntoArrayList();

        InsertPartsReplaceDate();
        if (!this.IsSuccess) {
            return;
        }


        CheckStatus(provider.status);
    }

    /**
     * for 料件更換週期主檔新增
     * 1. 型號 & 料件抓取 等於 問題表單的更換料件、統計型更換料件A、B、 C
     * 所有單據(只要有符合一個就要)
     * 2. 單據SerialNo, 更換日期， if SerialNo 重複，保留最新一筆更換日期
     * 3. SerialNo,MKeyNo 記錄到機台料件更換日期:紀錄欄位
     * 3.1: SerialNo
     * 7.2:MKeyNo:料件更換週期主檔
     * 7.3:上次更換日期:ticket的更換日期
     * 7.4:預計下次更換時間:上次更換日期+更換週期 * 7
     */
    void BatchCreatedLogic() {
        if (result != null && !result.isEmpty()) {
            result.clear();
        }
        //TODO:抓取MaterialId、SerialNo等資料
        SelectCreatedMaterialCycle();

        CheckStatus(provider.status);
        //如果狀態代碼錯誤，return
        if (this.status != 0) {
            return;
        }


        SelectChangeMaterialsFromTicket();


    }


    /**
     * for 料件更換週期主檔編輯:
     * 1. 更換週期異動:
     * 所有相同MKeyNo ，充新預計下次更換日期
     * 2. 停用註記:
     * 2.1: 無->有:
     * 將機台料件更換日期記錄檔中所有相同MkeyNo.的資料刪除。
     * 2.2:有->無:
     * 視為新增 執行 MaterialCycleCreate的內容
     */
    void BatchUpdatedLogic() {
        if (result != null && !result.isEmpty()) {
            result.clear();
        }

        SelectUpdatedMaterialCycle();
    }

    /**
     * 更新排程資料
     * if error -> 狀態 N -> E
     * if success ->狀態 N-> S
     */
    Boolean UpdateJobTask() {
        if (result != null && !result.isEmpty()) {
            result.clear();
        }

        String OutPutMsg = "維護管理平台系統-機台料件更換日期記錄檔，紀錄時間: " + Now;
        this.IsSuccess = provider.UpdateBatchJob(this.status, JobTaskId, this.error, this.userId, OutPutMsg, this.FireDateTime);

        return IsSuccess;
    }

    /**
     * 排程程序結束前。最後新增一筆明天的排程資訊
     */
    Boolean CreateTomorrowJobTask() {
        String InputParam = "{\"TICKET_ID\":\"" + Now + "\"}";
        this.IsSuccess = provider.InsertBatchJob(Tomorrow, this.userId, InputParam);
        return IsSuccess;
    }

    /**
     * 檢查回傳狀態代碼 status，<0，發生Exception
     * -99資料庫錯誤
     */
    void CheckStatus(Integer StatusCode) {
        if (StatusCode == -99) {
            this.status = provider.status;
            this.error = provider.error;
        }

        if (StatusCode < 0) {
            this.status = provider.status;
            this.error = provider.error;
        }

        this.CheckEntry.put("status", this.status);
        this.CheckEntry.put("error", this.error);
        this.Check.add(this.CheckEntry);
    }

    /**
     * 檢查回傳資料，若為空則狀態代碼為負數
     * 依照邏輯進行下一步的判斷，這邊不直接進行 Return
     */
    protected void CheckReturn(ArrayList<Map<String, Object>> arrayList) {
        if (arrayList == null || arrayList.isEmpty()) {
            this.status = -94;
            this.error = "資料回傳錯誤，不得為空";
        }
        this.CheckEntry.put("status", this.status);
        this.CheckEntry.put("error", this.error);
        this.Check.add(this.CheckEntry);
    }

    /**
     * 檢查list是否有重複的值
     * 有->true
     */
    protected Boolean IsRepeat(List<String> Input) {
        boolean Repeat;
        Repeat = Input.size() != new HashSet<>(Input).size();

        return Repeat;
    }
}
