package com.hepo.dfs.client.client;

/**
 * Description: 文件信息
 * Project:  mini-hdfs
 * CreateDate: Created in 2022/11/15 23:59
 *
 * @author linhaibo
 */
public class FileInfo {
    private String fileName;
    private Long fileLength;
    private byte[] file;

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Long getFileLength() {
        return fileLength;
    }

    public void setFileLength(Long fileLength) {
        this.fileLength = fileLength;
    }

    public byte[] getFile() {
        return file;
    }

    public void setFile(byte[] file) {
        this.file = file;
    }
}
