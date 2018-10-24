package util;

import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 *
 * 功能描述: 网络工具类帮助 修订版
 *
 * @param:
 * @return:
 * @auther: wuyue
 * @date: 2018/8/9 下午12:11
 */
public class NetUtil {

    /**
     *
     * 功能描述: 获取主机名
     *
     * @param:
     * @return:
     * @auther: wuyue
     * @date: 2018/8/9 下午9:12
     */
    public static String getHostname()  {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null ;
    }

    /**
     *
     * 功能描述: 返回进程pid
     *
     * @param:
     * @return:
     * @auther: wuyue
     * @date: 2018/8/9 下午9:12
     */
    public static String getPID(){
        String info = ManagementFactory.getRuntimeMXBean().getName();
        return info.split("@")[0] ;
    }

    /**
     *
     * 功能描述: 获取当前线程的名字
     *
     * @param:
     * @return:
     * @auther: wuyue
     * @date: 2018/8/9 下午9:12
     */
    public static String getTID(){
        return Thread.currentThread().getName() ;
    }

    /**
     *
     * 功能描述: 获取对象的信息
     *
     * @param:
     * @return:
     * @auther: wuyue
     * @date: 2018/8/9 下午9:13
     */
    public static String getOID(Object obj ){
        String cname = obj.getClass().getSimpleName();
        int hash = obj.hashCode() ;
        return cname + "@" + hash ;
    }

    public static String info(Object obj , String msg){
        return getHostname() + "," + getPID() + "," + getTID() + "," + getOID(obj) + "," + msg ;
    }

    /**
     *
     * 功能描述: 发送socket给远端server
     *
     * @param:
     * @return:
     * @auther: wuyue
     * @date: 2018/8/9 下午9:14
     */
    public static void sendToClient(Object obj,String msg,int port ){
        try {
            String info = info(obj,msg);
            Socket sock = new Socket("bigdata01.com", port);
            OutputStream os = sock.getOutputStream();
            os.write((info + "\r\n").getBytes());
            os.flush();
            os.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
