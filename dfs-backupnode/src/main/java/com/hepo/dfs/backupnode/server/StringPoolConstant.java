package com.hepo.dfs.backupnode.server;

/**
 * @author linhaibo
 * version: 1.0
 * Description:string的常量池
 **/
public class StringPoolConstant {

    /**
     * .分隔符
     */
    public static final String DOT = ".";

    /**
     * -分隔符
     */
    public static final String DASH = "-";

    /**
     * 下划线分隔符
     */
    public static final String UNDERLINE = "_";

    /**
     * 空格分隔符
     */
    public static final String EMPTY = "";

    /**
     * 冒号分隔符
     */
    public static final String COLON = ":";

    /**
     * 逗号分隔符
     */
    public static final String COMMA = ",";

    /**
     * 高亮前缀
     */
    public static final String PRE_TAG = "<span style=\"color:red\">";

    /**
     * 高亮后缀
     */
    public static final String POST_TAG = "</span>";

    /**
     * *号的模糊查询
     */
    public static final String STAR = "*";

    /**
     * 时间格式 yyyy-MM-dd HH:mm:ss
     */
    public static final String FULL_DATE_TIME = "yyyy-MM-dd HH:mm:ss";

    /**
     * id字段
     */
    public static final String ID = "id";

    /**
     * routing字段
     */
    public static final String ROUTING = "routing";

    /**
     * reindex的OpType参数值，index表示存在就修改，create表示不存在时才创建
     */
    public static final String CREATE = "create";

    /**
     * reindex时Conflicts参数值，proceed冲突时继续，abort冲突时终止
     */
    public static final String PROCEED = "proceed";


}
