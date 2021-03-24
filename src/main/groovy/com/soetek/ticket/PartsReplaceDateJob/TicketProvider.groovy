package com.soetek.ticket.PartsReplaceDateJob

import org.jetbrains.annotations.NotNull
import org.slf4j.Logger

class TicketProvider extends ConnectionProvider {
    // 提供resultFields判斷基準
    protected Integer status = 0
    protected String error = ""

    /**
     * 建構子
     * @param logger
     * @param jndiDB
     */
    TicketProvider(Logger logger, String jndiDB) {
        super(logger, jndiDB)
    }

/**
 * 邏輯1:
 * 建立 TEMP001
 * 今天&昨天新建立或者異動過的Ticket
 * @param yesterday 昨天
 *
 */
    ArrayList<Map<String, Object>> SelectTicket(@NotNull Yesterday, Boolean create, Boolean update) {
        GString selectTicket = GString.EMPTY +
                " SELECT ticket_id AS TicketId , number AS TicketNo, topic_id AS TopicId FROM  ost_ticket "

        GString whereClause = GString.EMPTY + " WHERE  1=1 "
        if (create) {
            whereClause += " AND created BETWEEN '${Yesterday}' AND NOW() "
        }

        if (update) {
            whereClause += " AND lastupdate BETWEEN '${Yesterday}' AND NOW() "
        }

        GString orderClause = GString.EMPTY + "ORDER BY ticket_id ;"
        GString gString = selectTicket + whereClause + orderClause

        return GetDataList(gString)
    }

    /**
     * 抓取客製表單內容
     * 為了得知，新增的料件是否已在案件中，
     */
    ArrayList<Map<String, Object>> SelectEntryValue(String LabelName) {
        GString selectMachineTypeInTicket = GString.EMPTY +
                " SELECT A.id AS fieldId , A.form_id, A.label ,\n" +
                "       B.id AS entryId, B.object_id AS ticketId, \n" +
                "       C.value FROM ost_form_field AS A \n" +
                "       LEFT JOIN ost_form_entry AS B ON A.form_id = B.form_id \n" +
                "       LEFT JOIN ost_form_entry_values AS C ON C.entry_id = B.id \n" +
                "       AND C.field_id = A.id \n" +
                "       WHERE A.label = '${LabelName}' " +
                "       AND C.value IS NOT NULL " +
                "       AND B.object_type = 'T' ;"

        return GetDataList(selectMachineTypeInTicket)
    }

    ArrayList<Map<String, Object>> SelectCountMaterial(String label) {
        GString selectStatisticMaterial = GString.EMPTY +
                " SELECT A.id AS fieldId , A.form_id, A.label , B.id AS entryId, B.object_id AS ticketId, C.value FROM ost_form_field AS A\n" +
                " LEFT JOIN ost_form_entry AS B ON A.form_id = B.form_id \n" +
                " LEFT JOIN ost_form_entry_values AS C ON C.entry_id = B.id \n" +
                " AND C.field_id = A.id WHERE A.label like '%${label}%'" +
                " AND C.value IS NOT NULL AND B.object_type = 'T' " +
                " ORDER BY A.label;"

        return GetDataList(selectStatisticMaterial)
    }
    /**
     * 清單中的id, value
     * 抓取機台型號的名稱，
     * 為了找到對應的料件
     */
    ArrayList<Map<String, Object>> SelectList(List<Integer> ListId, String ListName) {
        GString selectClause = GString.EMPTY +
                " SELECT A.id AS listId,A.name AS listName,B.id AS itemId, B.value AS listItem  FROM ost_list AS A "

        GString joinClause = GString.EMPTY +
                " LEFT JOIN ost_list_items AS B ON A.id = B.list_id "

        GString whereClause = GString.EMPTY + "WHERE B.value IS NOT NULL "

        if (ListId.size() > 0) {
            if (ListId.size() > 1) {
                whereClause += " AND B.id in ("
                for (int i = 0; i < ListId.size(); i++) {
                    def Id = ListId.get(i)
                    whereClause += Id + ","
                }
                whereClause = GString.EMPTY + whereClause.substring(0, whereClause.length() - 1)
                whereClause += ")"
            }
            if (ListId.size() == 1) {
                def id = ListId.get(0)
                whereClause += " AND B.id = $id "
            }
        }

        if (!ListName.isEmpty()) {
            whereClause += " AND A.name = ${ListName} "
        }

        GString orderClause = GString.EMPTY + "ORDER BY B.id ;"
        GString gString = selectClause + joinClause + whereClause + orderClause

        return GetDataList(gString)
    }

    /**
     * 1.抓取 MaterialCycle
     * 2.與ticket的料件配對
     * 3.判斷在今天是否新增or修改資料
     * @param MaterialIdList 料件id
     * @param create 查詢新增
     * @param update 查詢更新
     * @param todayStart 前一天零點
     * @return
     */
    ArrayList<Map<String, Object>> SelectMaterialCycle(List<Integer> MaterialIdList, Boolean create, Boolean update, String YesterdayStart) {
        GString selectClause = GString.EMPTY +
                " SELECT Id, MaterialId, MKeyNo, ChangeCycle, DeleteFlag " +
                " FROM soe_MaterialCycle "

        GString whereClause = GString.EMPTY + " WHERE 1 = 1 "

        if (MaterialIdList != null && !MaterialIdList.isEmpty()) {
            if (MaterialIdList.size() > 1) {
                whereClause += " AND MaterialId IN ( "
                for (int i = 0; i < MaterialIdList.size(); i++) {
                    def Id = MaterialIdList.get(i)
                    whereClause += Id + ","
                }
                whereClause = GString.EMPTY + whereClause.substring(0, whereClause.length() - 1)
                whereClause += ")"
            }
            if (MaterialIdList.size() == 1) {
                def Id = MaterialIdList.get(0)
                whereClause += " AND MaterialId = $Id "
            }
        }

        if (create) {
            whereClause += " AND (Created BETWEEN '${YesterdayStart}' AND NOW()) "
        }

        if (update) {
            whereClause += " AND (UPDATED BETWEEN '${YesterdayStart}' AND NOW()) "
        }
        whereClause += " ORDER BY Id; "
        GString gString = selectClause + whereClause
        return GetDataList(gString)

    }

    /**
     * 抓取與 ticket對應的 欄位值
     * @param ticketIdList
     * @param ChangeMaterial 更換料件
     * @param SerialNo 、統計型更換料件A、B、C
     * @param yesterday
     * @param repeat for SerialNo重複
     * @param changeValue 重複的 SerialNo -> 最大的更換時間日期
     */
    ArrayList<Map<String, Object>> SelectFormEntryValueByTicket(String LabelName, List<Integer> ticketIdList, Yesterday, Boolean create, Boolean update, Boolean repeat, List<String> changeValue) {
        GString selectClause = GString.EMPTY +
                " SELECT B.object_id AS ticketId,\n" +
                "       C.value FROM ost_form_field AS A \n"

        GString joinClause = GString.EMPTY +
                "       LEFT JOIN ost_form_entry AS B ON A.form_id = B.form_id \n" +
                "       LEFT JOIN ost_form_entry_values AS C ON C.entry_id = B.id \n" +
                "       AND C.field_id = A.id "

        GString whereClause = GString.EMPTY + " WHERE 1 = 1 "

        if (LabelName != null && !LabelName.isEmpty()) {
            whereClause += " AND A.label = '${LabelName}' \n"
        }
        whereClause = GString.EMPTY + whereClause.substring(0, whereClause.length() - 1)
        if (!repeat) {
            whereClause += " AND C.value IS NOT NULL \n"
        } else {
            whereClause += "  AND C.value =  "
        }
        whereClause += " AND B.object_type = 'T' \n"

        if (ticketIdList.size() > 0) {
            if (ticketIdList.size() == 1) {
                def TicketList = ticketIdList.get(0)
                whereClause += " AND B.object_id = $TicketList "
            }
            if (ticketIdList.size() > 1) {
                whereClause += "AND B.object_id IN ("
                for (Integer ticketId : ticketIdList) {
                    whereClause += ticketId + ","
                }
                whereClause = GString.EMPTY + whereClause.substring(0, whereClause.length() - 1)
                whereClause += " ) "
            }
        }

        if (create) {
            whereClause += " AND B.created BETWEEN '${Yesterday}' AND NOW() "
        }
        if (update) {
            whereClause += " AND B.updated BETWEEN '${Yesterday}' AND NOW() "
        }
//        whereClause = GString.EMPTY + whereClause.substring(0, whereClause.length() - 1)
        whereClause += ";"

        GString gString = selectClause + joinClause + whereClause
        GetDataList(gString)
    }


    ArrayList<Map<String, Object>> LatestChangeDate(List<Integer> TicketList, String LabelName) {
        GString gString = GString.EMPTY +
                ""
    }


    /**
     * 新增BatchJob
     * 今天排程結束前，最後新增一筆明天的日常排程
     */
    protected Boolean InsertBatchJob(@NotNull BatchExpStart, String userId, String InputParam) {
        GString gString = GString.EMPTY +
                "INSERT INTO soe_BatchJob (" +
                " FuncCode," +
                " ObjectType," +
                " InParam," +
                " ExpectedTime," +
                " ActualStart," +
                " FunStatus," +
                " Created," +
                " CreateBy" +
                " )" +
                "  VALUES (" +
                " 'FC3'," +
                "  'T'," +
                "  '${InputParam}'," +
                "  '${BatchExpStart}'," +
                "  now()," +
                " 'N'," +
                "  now()," +
                "  '${userId}'" +
                " );"

        return ExecuteData(gString)
    }

    /**
     * 查詢今天的排程(尚未執行)
     * 目的:查詢今日要執行的排程有哪些，根據InParam分辨邏輯
     * Routine、 Create、Update
     * 用於更新排程狀態 & 錯誤訊息 & 昨天是否已經新增今天的排程
     * @param startDateTime 今天排程預計開始時間
     * @param userId = UserId + startDateTime(今天排程預計開始時間)
     * @return Id、InParam
     */
    ArrayList<Map<String, Object>> SelectBatchJob(startDateTime, String userId) {
        GString selectClause = GString.EMPTY +
                " SELECT Id " +
                " ,InParam " +
                "  FROM soe_BatchJob" +
                "  WHERE FunStatus = 'N' "

        GString whereClause = GString.EMPTY + " AND FuncCode = 'FC3' "

        if (startDateTime != null) {
            whereClause += " AND ExpectedTime = '${startDateTime}' "
        }

        if (userId != null && !userId.isEmpty()) {
            whereClause += " AND CreateBy = '${userId}'"
        }

        GString gsQuery = selectClause + whereClause

        return GetDataList(gsQuery)
    }

    /**
     * 更新 BatchJob，
     * 依照依照status =0 or !=0
     * 改為 S or E
     * @param status 狀態代碼
     * @param Id JobTaskId
     * @param Output 排程成功，輸入的資料
     * @param FireTime 排程實際啟動時間
     * @error 排程啟動失敗的* @UserId 排程建立人 SysAuto+TodayDateTime
     */
    Boolean UpdateBatchJob(@NotNull Integer status, @NotNull List<Integer> Id, @NotNull String error, @NotNull String UserId, String Output, String FireTime) {

        GString FireTimeClause = GString.EMPTY +
                " UPDATE soe_BatchJob SET " +
                " ActualStart = '${FireTime}', " +
                " ActualEnd = NOW() "

        GString ConditionClause = GString.EMPTY + ","

        if (status == 0) {
            ConditionClause += "FunStatus = 'S',ExceptionText = '', OutParam = '${Output}',"
        } else {
            ConditionClause += "FunStatus = 'E', ExceptionText ='${error}',"
        }

        GString UserIdClause = GString.EMPTY +
                " Updated = NOW() , " +
                " UpdateBy = '${UserId}' " +
                " WHERE FuncCode = 'FC3' "

        GString TaskId = GString.EMPTY + " AND Id "
        if (Id.size() == 1) {
            def Ids = Id.get(0)
            TaskId += " = ${Ids}"
        }
        if (Id.size() > 1) {
            TaskId += " in ("
            for (int i = 0; i < Id.size(); i++) {
                def Ids = Id.get(i)
                TaskId += " ${Ids} " + ","
            }
            TaskId = GString.EMPTY + TaskId.substring(0, TaskId.length() - 1)
            TaskId += ") "
        }

        GString UpdateString = FireTimeClause + ConditionClause + UserIdClause + TaskId + ";"
        return ExecuteData(UpdateString)
    }

    /**
     * for 確認 PartsReplaceDate資料是否已經有資料
     * @param MKeyNo
     * @param SerialNo
     * @parama ChangedDate 上次更換日期
     * @param NextDate 下次更換日期
     */
    ArrayList<Map<String, Object>> SelectPartsReplaceDate(List<String> MKeyNo, List<String> SerialNo) {
        GString selectClause = GString.EMPTY +
                " SELECT Id,SerialNo,MKeyNo,PreviousDate FROM soe_PartsReplaceDate "
        GString whereClause = GString.EMPTY + " WHERE  1 = 1"
        if (MKeyNo.size() == 1) {
            def key = MKeyNo.get(0)
            whereClause += " AND MKeyNo = $key "
        }

        if (MKeyNo.size() > 1) {
            whereClause += " AND MKeyNo IN ("
            for (int i = 0; i < MKeyNo.size(); i++) {
                def key = MKeyNo.get(i)
                whereClause += " ${key} " + ","
            }
            whereClause = GString.EMPTY + whereClause.substring(0, whereClause.length() - 1)
            whereClause += ")"
        }

        if (SerialNo.size() == 1) {
            def serial = SerialNo.get(0)
            whereClause += " AND SerialNo = $serial"
        }

        if (SerialNo.size() > 1) {
            whereClause += " AND SerialNo IN ( "
            for (int i = 0; i < SerialNo.size(); i++) {
                def serial = SerialNo.get(i)
                whereClause += " '${serial}'"
            }
            whereClause = GString.EMPTY + whereClause.substring(0, whereClause.length() - 1)
            whereClause += ")"
        }
        whereClause += " AND DeleteFlag IS NULL;"

        GString gString = GString.EMPTY + selectClause + whereClause
        return GetDataList(gString)
    }

    /**
     * 輸入資料至 PartsReplaceDateJob
     * */
    //ToDo:新增輸入TicketId，確認sql這樣是否正確
    Boolean InsertPartsReplaceDate(@NotNull List<String> SerialNo, @NotNull List<String> MKeyNo, @NotNull List<String> PreviousDate, List<Integer> TicketIdList, @NotNull List<String> NextDate, @NotNull String CreateBy) {
        GString insert = GString.EMPTY +
                "INSERT INTO soe_PartsReplaceDate \n" +
                "( SerialNo , MKeyNo , PreviousDate ,PreviousTicketId, NextDate , Created , CreateBy) " +
                "VALUES "
        //SerialNo
        GString insertSerial = GString.EMPTY + "("
        //MKeyNo
        GString insertMKey = GString.EMPTY + ","
        //更換日期
        GString insertPreviousDate = GString.EMPTY + ","
        //取得更換日期的那張TicketId
        GString insertPreviousTicketId = GString.EMPTY + ","
        //下次更換日期
        GString insertNextDate = GString.EMPTY + ","
        //最後的insert(create, CreatedBy)
        GString lastInsert = GString.EMPTY + ","

        if (SerialNo.size() > 1 || MKeyNo.size() > 1 || PreviousDate.size() > 1 || TicketIdList.size() > 1 || NextDate.size() > 1) {
            //SerialNo
            if (SerialNo.size() == 1) {
                def SERNO = SerialNo.get(0)
                insertSerial += "$SERNO"
            }

            if (SerialNo.size() > 1) {
                for (int i = 0; i < SerialNo.size(); i++) {
                    def SERNO = SerialNo.get(i)
                    insertSerial += "'${SERNO}'"
                }
            }

            //MKeyNo
            if (MKeyNo.size() == 1) {
                def MKey = MKeyNo.get(0)
                insertMKey += "'${MKey}'"
            }

            if (MKeyNo.size() > 1) {
                for (int j = 0; j < MKeyNo.size(); j++) {
                    def MKey = MKeyNo.get(j)
                    insertMKey += " $MKey "
                }
                insertMKey = GString.EMPTY + insertMKey.substring(0, insertMKey.length() - 1)
            }

            //更換日期
            if (PreviousDate.size() == 1) {
                def PDate = PreviousDate.get(0)
                insertPreviousDate += " '$PDate' "
            }

            if (PreviousDate.size() > 1) {
                for (int k = 0; k < PreviousDate.size(); k++) {
                    def PDate = PreviousDate.get(k)
                    insertPreviousDate += " '$PDate' "
                }
                insertPreviousDate = GString.EMPTY + insertPreviousDate.substring(0, insertPreviousDate.length() - 1)
            }

            //案件ID
            if (TicketIdList.size() == 1) {
                def id = TicketIdList.get(0)
                insertPreviousTicketId += " $id "
            }

            if (TicketIdList.size() > 1) {
                for (int i = 0; i < TicketIdList.size(); i++) {
                    def id = TicketIdList.get(i)
                    insertPreviousTicketId += " $id "
                }
                insertPreviousTicketId = GString.EMPTY + insertPreviousTicketId.substring(0, insertPreviousTicketId.length() - 1)
            }


            //下次更換日期
            if (NextDate.size() == 1) {
                def next = NextDate.get(0)
                insertNextDate += " $next "
            }

            if (NextDate.size() > 1) {
                for (int q = 0; q < NextDate.size(); q++) {
                    def next = NextDate.get(q)
                    insertNextDate += " $next "
                }
            }
        }

        //輸入條件的size全部 == 1的情況
        if (SerialNo.size() == 1 && MKeyNo.size() == 1 && PreviousDate.size() == 1 && TicketIdList.size() == 1 && NextDate.size() == 1) {
            //SerialNo
            def SERNO = SerialNo.get(0)
            insertSerial += "$SERNO"

            //MKeyNo
            def MKey = MKeyNo.get(0)
            insertMKey += "'${MKey}'"

            //更換日期
            def PDate = PreviousDate.get(0)
            insertPreviousDate += " '$PDate' "

            //案件ID
            def id = TicketIdList.get(0)
            insertPreviousTicketId += " $id"

            //下次更換日期
            def next = NextDate.get(0)
            insertNextDate += " $next "
        }
        lastInsert += " NOW(), '${CreateBy}' );"

        GString gString = insert + insertSerial + insertMKey + insertPreviousDate + insertPreviousTicketId + insertNextDate + lastInsert
        return ExecuteData(gString)
    }


    Boolean test(@NotNull ArrayList<Map<String, Object>> Temp) {
        GString gSql = GString.EMPTY + "temporary table temp02(select SerialNo,MKeyNo,PreviousDate,PreviousTicketId,NextDate,DeleteFlag,Created,CreateBy from soe_PartsReplaceDate)" +
                " insert into temp02 (SerialNo,MKeyNo,PreviousDate,PreviousTicketId,NextDate,DeleteFlag,Created,CreateBy) "

        GString insertSql = GString.EMPTY + "values ("
        if (Temp.size() == 1) {
            def SerialNo = Temp.get(0).get("SerialNo")
            def MKeyNo = Temp.get(0).get("MKeyNo")
            def PreviousDate = Temp.get(0).get("PreviousDate")
            def NextDate = Temp.get(0).get("NextDate")
            def DeleteFlag = Temp.get(0).get("DeleteFlag")

            insertSql += " '$SerialNo', '$MKeyNo' ,'$PreviousDate' ,'$NextDate', $DeleteFlag, now(), 'AUT001' "
        }

        if (Temp.size() > 1) {
            for (int i = 0; i < Temp.size(); i++) {
                def SerialNo = Temp[i].get("SerialNo")
                def MKeyNo = Temp[i].get("MKeyNo")
                def PreviousDate = Temp[i].get("PreviousDate")
                def NextDate = Temp[i].get("NextDate")
                def DeleteFlag = Temp[i].get("DeleteFlag")

                insertSql += " '${SerialNo}',"
                insertSql += " '${MKeyNo}',"
                insertSql += " '$PreviousDate',"
                insertSql += " '$NextDate',"
                insertSql += " $DeleteFlag"

                insertSql += "),"
            }
            insertSql = GString.EMPTY + insertSql.substring(0, insertSql.length() - 1)
            insertSql += ";"
        }

        GString gString = gSql + insertSql
        return ExecuteData(gString)
    }

}
