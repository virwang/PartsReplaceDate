package com.soetek.ticket.PartsReplaceDateJob

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter


class SimpleConvertHelper {
    DateTimeFormatter DateFormat = DateTimeFormatter.ofPattern('yyyy-MM-dd')
    DateTimeFormatter DateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
    DateTimeFormatter TimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    SimpleConvertHelper() {}

/**
 * LocalDate To String
 * @param LocalDate
 * @return String yyyy-MM-dd
 */
    String LocalDateToString(LocalDate Input) {
        String Output = Input.format(DateFormat)
        return Output
    }

    /**
     * String To LocalDate
     * @param String
     * @return LocalDate yyyy-MM-dd
     */
    LocalDate StringToLocalDate(String Input) {
        LocalDate Output = LocalDate.parse(Input, DateFormat)
        return Output
    }

    /**
     * String To Integer
     * @param String
     * @return Integer
     */
    Integer StringToInteger(String Input) {
        Integer Output = Integer.parseInt(Input)
        return Output
    }

    /**
     * String To Float
     * @param String
     * @return Float
     */
    Float StringToFloat(String Input) {
        Float Output = Float.parseFloat(Input)
        return Output
    }

    /**
     * String To LocalDateTime
     * @param String
     * @return LocalDateTime
     */
    LocalDateTime StringToLocalDateTime(String Input) {
        LocalDateTime Output = LocalDateTime.parse(Input, TimeFormat)
        return Output
    }

    /**
     * LocalDateTime to String
     * @param LocalDateTime
     * @return String
     */
    String LocalDateTimeToString(LocalDateTime Input) {
        String Output = Input.format(TimeFormat)
        return Output
    }

    String DateToLocalDateTime(Date Input) {
        LocalDateTime Output = LocalDateTime.ofInstant(Input.toInstant(), ZoneId.systemDefault())
        return Output
    }
}
