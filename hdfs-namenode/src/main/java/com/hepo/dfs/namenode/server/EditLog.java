package com.hepo.dfs.namenode.server;

import com.alibaba.fastjson.JSONObject;

/**
 * editLog内部类，代表一条日志
 */
public class EditLog {
    /**
     * 当前日志的txid
     */
    Long txid;
    /**
     * 日志内容
     */
    String content;

    public EditLog(Long txid, String content) {
        this.txid = txid;
        JSONObject jsonObject = JSONObject.parseObject(content);
        jsonObject.put("txid", txid);
        this.content = jsonObject.toJSONString();
    }

    public Long getTxid() {
        return txid;
    }

    public void setTxid(Long txid) {
        this.txid = txid;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}