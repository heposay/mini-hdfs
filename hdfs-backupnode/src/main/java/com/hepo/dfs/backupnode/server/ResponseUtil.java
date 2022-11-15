package com.hepo.dfs.backupnode.server;

/**
 * Description:
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/2 15:40
 *
 * @author linhaibo
 */
public class ResponseUtil {


    public static String getMsg(int statusCode) {
        return ResponseStatusEnum.getMsgByStatus(statusCode);
    }

    enum ResponseStatusEnum {
        SUCCESS(1, "SUCCESS"),
        FAILURE(2, "FAILURE"),
        SHUTDOWN(3, "SHUTDOWN"),
        DUPLICATE(4, "DUPLICATE"),
        ;
        private Integer status;
        private String msg;

        ResponseStatusEnum(Integer status, String msg) {
            this.status = status;
            this.msg = msg;
        }

        public Integer getStatus() {
            return status;
        }

        public String getMsg() {
            return msg;
        }

        public static String getMsgByStatus(int status) {
            for (ResponseStatusEnum value : values()) {
                if (value.getStatus() == status) {
                    return value.getMsg();
                }
            }
            return "ERROR";
        }
    }


}
