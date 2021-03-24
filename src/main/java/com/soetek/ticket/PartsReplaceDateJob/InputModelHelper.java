package com.soetek.ticket.PartsReplaceDateJob;

public class InputModelHelper {
    Integer MaterialId;
    Integer ChangeCycle;
    String MKeyNo;

    public InputModelHelper(Integer materialId, Integer changeCycle, String MKeyNo) {
        MaterialId = materialId;
        ChangeCycle = changeCycle;
        this.MKeyNo = MKeyNo;
    }
}