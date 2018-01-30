package org.monitor.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
/**
 * @author HuangGang
 */
public class MonitorDistribution {
    //sftp对象
    public static SFTP sftp = new SFTP();
    //文件服务器数量
    static int count = Integer.parseInt(ConfigUtils.getValue("fileServer.count"));
    //本地文件暂存目录
    static String localPath = ConfigUtils.getValue("fileLocal.Path");
    //文件阈值
    static int fileMaxCount = Integer.parseInt(ConfigUtils.getValue("fileMaxCount"));
    //文件服务器转移文件数量
    static int fileTransferCount = Integer.parseInt(ConfigUtils.getValue("fileTransferCount"));
    //扫描时间间隔
    static int fileActionTimer = Integer.parseInt(ConfigUtils.getValue("fileActionTimer"));
    //文件服务器文件数
    static int[] filecount= new int[count];

    Object[] objs = null;

    /**
     * 获取sftp连接通道
     * @param --ftpIp 	服务器ip
     * @param --ftpUser	服务器用户名
     * @param --ftpPass	服务器密码
     * @param --ftpPort	服务器端口
     * @param --dir		文件所在目录
     * @throws Exception
     */
    public ChannelSftp getConectChanel(String ftpIp, String ftpUser,String ftpPass,String ftpPort,String dir) throws Exception {
        ChannelSftp ch = null;
        return ch = sftp.connect(ftpIp, ftpUser, ftpPass, Integer.valueOf(ftpPort), dir);
    }




    /**
     * 获取文件服务器的文件数量
     * @param --sourceChannel 	文件sftp连接通道
     * @param --dir			文件服务器文件目录
     * @return	--int			文件个数
     * @throws --Exception
     */
    public int getFileCount(ChannelSftp ch,String dir) throws Exception {
        List list = sftp.list(ch);
        sftp.disconnect(ch);

        return list.size();
    }



    /**
     * 获取文件到本地并转发至服务器
     * @param --sourceChannel 源服务器sftp通道
     * @param --targetChannel	目标服务器sftp通道
     * @param --sourcePath	源服务器文件目录
     * @param --targetPath	目标服务器文件目录
     * @param --destination	本地临时文件存放目录
     * @param --batchId		批次Id
     * @param --sourceIp		本地临时文件存放目录
     * @param --targetIp		本地临时文件存放目录
     * @throws Exception
     */
    public void downLoadFileForward(ChannelSftp sourceChannel,String sourcePath,String destination,String sourceIp) throws Exception {
        List list = sftp.list(sourceChannel);
        objs = sortFile(list);
        for (int i = 0; i < fileTransferCount; i++) {
            System.out.println("index="+i);
            Map map = (Map)objs[i];
            String name = (String)map.get("name");
            File file = new File(destination);
            if(!file.exists()){
                file.mkdirs();
            }
            FileOutputStream downloadFile = new FileOutputStream(destination+"/"+name);
            String fullPath = sourcePath + "/" + name;
            sourceChannel.cd(sourcePath);
            InputStream input = sourceChannel.get(fullPath);
            int index;
            byte[] bytes = new byte[1024];
            while ((index = input.read(bytes)) != -1) {
                downloadFile.write(bytes, 0, index);
                downloadFile.flush();
            }
            downloadFile.close();
            input.close();
            System.out.println("下载结束");
            delete(sourcePath, sourceChannel, name);
        }

    }
    /**
     * 获取文件到本地并转发至服务器
     * @param --sourceChannel 源服务器sftp通道
     * @param --targetChannel	目标服务器sftp通道
     * @param --sourcePath	源服务器文件目录
     * @param --targetPath	目标服务器文件目录
     * @param --destination	本地临时文件存放目录
     * @param --batchId		批次Id
     * @param --sourceIp		本地临时文件存放目录
     * @param --targetIp		本地临时文件存放目录
     * @throws Exception
     */
    public void uploadFileForward(ChannelSftp targetChannel,String targetPath,String destination,String batchId,String sourceIp,String targetIp) throws Exception {
        for (int i = 0; i < fileTransferCount; i++) {
            System.out.println("index="+i);
            Date d = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Map map = (Map)objs[i];
            String name = (String)map.get("name");
            upload(targetPath, destination+"/"+name, targetChannel);
            File filedelete = new File(destination+"/"+name);
            deleteDir(filedelete);
        }

    }
    /**
     * 远程文件排序
     * @param --list
     * @return
     */
    private Object[] sortFile(List list) {
        Object[] objs = list.toArray();
        Map temp;
        for (int i = 0; i < objs.length; i++) {// 趟数
            for (int j = 0; j < objs.length - i - 1; j++) {// 比较次数
                if (((String) ((Map) objs[j]).get("name"))
                        .compareTo((String) ((Map) objs[j + 1]).get("name")) > 0) {
                    temp = (Map) objs[j];
                    objs[j] = objs[j + 1];
                    objs[j + 1] = temp;
                }
            }
        }

        return objs;
    }
    /**
     * 删除本地暂存文件
     * @param --dir	本地暂存文件目录
     * @return
     */
    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            System.out.println("111");
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                System.out.println(success+"..."+i);
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }
    /**
     * 文件上传
     * @param --directory 目录
     * @param --uploadFile 要上传的文件名
     * @param --sftp
     */
    public void upload(String directory, String uploadFile, ChannelSftp sftp) {
        try {
            sftp.cd(directory);
            File file = new File(uploadFile);
            FileInputStream fin= new FileInputStream(file);
            sftp.put(fin, file.getName());
            fin.close();
            System.out.println("上传成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 远程文件删除功能
     * @param --directory 远程文件位置
     * @param --sftp		sftp通道
     * @throws SftpException
     */
    public void delete(String directory, ChannelSftp ch, String filename) throws SftpException{
        ch.rm(directory+"/"+filename);
    }

    /**
     * 获取最少文件数的服务器编号
     * @param --prams
     * @return
     */
    public int getMinFileCount(int [] prams){
        int min = 10000;
        int index = 0;
        for(int i=0;i<prams.length;i++){
            if(min > prams[i]){
                min = prams[i];
                index = i;
            }
        }
        return index;
    }
    /**
     * 获取最大文件数的服务器编号
     * @param --prams
     * @return
     */
    public int getMaxFileCount(int [] prams){
        int max = 0;
        int index = 0;
        for(int i=0;i<prams.length;i++){
            if(max < prams[i]){
                max = prams[i];
                index = i;
            }
        }
        return index;
    }
    /**
     * 控制文件扫描及文件转移
     */
    public void action(){
        try {
            for(int i=0;i<count;i++){
                String ftpIp = ConfigUtils.getValue("fileServer.IP"+i);
                String ftpPort = ConfigUtils.getValue("fileServer.Port"+i);
                String ftpUser = ConfigUtils.getValue("fileServer.User"+i);
                String ftpPass = ConfigUtils.getValue("fileServer.Pass"+i);
                String dir = ConfigUtils.getValue("fileServer.Path"+i);
                //获取文件数量
                ChannelSftp sourceChanel = getConectChanel(ftpIp, ftpUser, ftpPass, ftpPort, dir);
                filecount[i] = getFileCount(sourceChanel,dir);
                System.out.println(filecount[i]);
            }
            int max = getMaxFileCount(filecount);
            Date start = new Date();
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            System.out.println(format.format(start));

            if(filecount[max]>fileMaxCount){
                int min = getMinFileCount(filecount);
                UUID uuid = UUID.randomUUID();
                String batchId = uuid.toString().replaceAll("\\-", "");
                String forwardMinIp = ConfigUtils.getValue("fileServer.IP"+min);
                String forwardMinPort = ConfigUtils.getValue("fileServer.Port"+min);
                String forwardMinUser = ConfigUtils.getValue("fileServer.User"+min);
                String forwardMinPass = ConfigUtils.getValue("fileServer.Pass"+min);
                String forwardMindir = ConfigUtils.getValue("fileServer.Path"+min);

                String forwardMaxIp = ConfigUtils.getValue("fileServer.IP"+max);
                String forwardMaxPort = ConfigUtils.getValue("fileServer.Port"+max);
                String forwardMaxUser = ConfigUtils.getValue("fileServer.User"+max);
                String forwardMaxPass = ConfigUtils.getValue("fileServer.Pass"+max);
                String forwardMaxdir = ConfigUtils.getValue("fileServer.Path"+max);
                ChannelSftp chanel = getConectChanel(forwardMaxIp,forwardMaxUser,forwardMaxPass,forwardMaxPort,forwardMaxdir);
                ChannelSftp chanel2 = getConectChanel(forwardMinIp,forwardMinUser,forwardMinPass,forwardMinPort,forwardMindir);
                //将文件发送至另一台服务器
                downLoadFileForward(chanel,forwardMaxdir,localPath,forwardMinIp);
                uploadFileForward(chanel2,forwardMindir,localPath,batchId,forwardMaxIp,forwardMinIp);
            }
            Date end = new Date();
            System.out.println(format.format(end));
            System.out.println(end.getTime()-start.getTime());
        } catch (Exception e) {
            // TODO 自动生成的 catch 块
            e.printStackTrace();
        }
    }
    /**
     * 定时器执行扫描及文件转移
     */
    public static void timerAction() {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            public void run() {
                System.out.println("-------扫描开始--------");
                MonitorDistribution m = new MonitorDistribution();
                m.action();
            }
        }, 0, fileActionTimer*1000);
    }
    public static void main(String[] args) {
        timerAction();
    }
}
