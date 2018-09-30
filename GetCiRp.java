package com.ifeng.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class GetCiRp extends UDF {
    public static String changeuri(String uri)
    {
        if (uri.equals("#")) {
            return "#";
        }
        if (uri.contains("?")) {
            uri = uri.split("\\?")[0];
        }
        if (uri.contains("#")) {
            uri = uri.split("#")[0];
        }
        uri = uri + "_zd";
        String[] uri1 = uri.split("/");
        String url2 = "";
        for (int i = 0; i < uri1.length - 1; i++)
        {
            url2 = url2 + uri1[i] + "/";
        }
        return url2;
    }
    public static int getIndexOf(String [] arr,String value){
        for (int i = 0; i < arr.length; i++) {
            if (arr[i].equals(value)) {
                return i;
            }
        }
        return -1;
    }
    public static Text evaluate(Text urls, Text ci)
    {

        try
        {
            if ((ci.toString() == null) || (ci.toString().equals("#"))) {
                return new Text(changeuri(urls.toString()));
            }
            String url = changeuri(urls.toString());
            String cia = ci.toString();
            if (ci.toString().contains(","))
            {
                cia = ci.toString().split(",")[0];
                if (cia.contains("?")) {
                    cia = cia.split("\\?")[0];
                }
                if (cia.contains("#")) {
                    cia = cia.split("#")[0];
                }
            }

            if(cia!=null &&cia.toString().contains(".ifeng")&&url.toString().contains(".ifeng")&&!url.toString().contains("http://.ifeng")){
                String [] cia1 = cia.split("//");
                String [] url1 = url.split("//");
                String [] ciaarry = cia1[1].split("\\.");
                String [] urlarry = url1[1].split("\\.");
                ciaarry[getIndexOf(ciaarry,"ifeng")-1] = urlarry[getIndexOf(urlarry,"ifeng")-1];
                return new Text("http://"+ StringUtils.join(ciaarry,"."));
            }else{
                return new Text(cia);
            }
        }
        catch (Exception e) {
        }
        return new Text(changeuri(urls.toString()));
    }

    public static void main(String[] args) {
        Text urls = new Text("http://wemedia.ifeng.ifeng.com/76190386/wemedia.shtml");
        Text ci = new Text("http://feng.ifeng.com/listpage/500638/author.shtml,,zmt_500638,fhh_76190386");
        System.out.println(evaluate(urls,ci));
    }
}
