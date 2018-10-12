import com.sun.org.apache.xpath.internal.SourceTree;
import scala.tools.nsc.backend.icode.Primitives;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2018/10/1 0001.
 */

interface ProcessFile {
//    public void process(String filePath, String dictPath);

    public Object process(String filePath, String dictPath);
}

//专用于发现中文字符串，并写入本地
class ServiceFindChineseChar implements ProcessFile {

    @Override
    public Object process(String filePath, String dictPath) {
        //        String path = "C:\\Users\\Administrator\\Desktop\\国际化cl.csv";
        String s = GetFiles.loadDict(dictPath);
        String[][] dictMap = GetFiles.dictToTwodArray(s);
        String contents = GetFiles.readTxtFile(filePath);

        String deleteComment = GetFiles.deleleComment(contents);
        //删除注释代码，含//, /* */, /** */这些模式
        String[] lines = deleteComment.split("\r\n");
//        String[] lines = contents.split("\r\n");
//        String chineseReg =  "[^\u4e00-\u9fa5+]";
        String chineseReg =  "[\u4e00-\u9fa5]";

        StringBuilder sb = new StringBuilder();
//        Matcher matcher = GetFiles.matchRegex(content, chineseReg);
        for (int row=0;row < lines.length; row++) {
            Matcher matcher = GetFiles.matchRegex(lines[row], chineseReg);
            if( matcher.find()) {
//                原始的将英文替换掉
//                String filterNotChinese = lines[row].replaceAll("[^\u4e00-\u9fa5+]", "@");
                //现在将所有英文字母段替换成@
//                String filterNotChinese = lines[row].replaceAll("[a-zA-Z]+", "@");
//                String filterNotChinese = lines[row].replaceAll("\\", "@");
//                //将所有  "\\"[\u4e00-\u9fa5]+\\""提取出来即可————————发现：所有的源码中的汉字，都用双引号或单引号，括起来了
//                sb.append(filterNotChinese + "\r\n");
//                sb.append(lines[row] + "\r\n");
                GetFiles.extractChineseFromLine(lines[row],sb);
                sb.append("\r\n");
            }
        }
        return (Object)sb.toString();
//        String outputPath = "";
//        GetFiles.writeFile(sb.toString(), outputPath);
    }

}




class  SortByStrLength implements Comparator {

    @Override
    public int compare(Object o1, Object o2) {
        {
            String str1 = (String) o1;
            String str2 = (String) o2;

            int num = str1.length() - str2.length();
            if(num == 0) {
                return str1.compareTo(str2);
            }
            return num;
        }
    }
}

public class GetFiles {
    //获取每行中的中文——这些中文，一般被',",<,>这4种包起来
    public static void extractChineseFromLine(String line, StringBuilder sb) {
        String[] split1 = line.split("[\"|\'|<|>]");
        for(int i = 0;i<split1.length;i++) {
            if (Pattern.compile("[\\u4e00-\\u9fa5]").matcher(split1[i]).find()) {
                sb.append(split1[i]+"@");
            }
        }
    }



    //删除注释，跨行删除，采用贪婪模式
    //https://blog.csdn.net/Pompeii/article/details/25168311
    public static String deleleComment(String context) {

        StringBuilder sb = new StringBuilder();
//        替换掉java的多行及文档注释, 这貌似只能替换单行的
//        String result = context.replaceAll("/\\*.*\\*/", "");
//        String result2 = context.replaceAll("/\\*(.|[\r\n])*?\\*/|<!--(.|[\r\n])*?-->|<%--(.|[\r\n])*?%-->","");

        String result = context.replaceAll("/\\*(.|[\r\n])*?\\*/", "");
        //删除一至多行注释，可以跨行，贪婪模式
        String result1 = result.replaceAll("<!--(.|[\r\n])*?-->","");
        //删除jsp的一至多行的注释
//        String result2 = result.replaceAll("<%--(.|[\r\n])*?%-->","");
//        System.out.println(result1);
        String[] split = result1.split("\r\n");
        for(int i=0; i<split.length; i++) {
            String filter = split[i].replaceAll("//.*", "");
            sb.append(filter + "\r\n");
        }
        return sb.toString();
    }


    public static ArrayList<String> getFileNames(String dir) {
        ArrayList<String> fileNames = new ArrayList<String>();
        File file = new File(dir);
//        System.out.println("go");
        if (file == null || !file.exists()) {
            return fileNames;
        }
//        LinkedList<File> list = new LinkedList<>();
        ArrayList<String> list = new ArrayList<String>();
        File[] files = file.listFiles();
        for (File it : files) {
            if (!it.isDirectory()) {
                fileNames.add(it.getAbsolutePath());
            }
        }
//        System.out.println("fhfh");
        return fileNames;
    }


    public static void traverseFolder2(String path, String dictPath) {

        File file = new File(path);
        if (file.exists()) {
            File[] files = file.listFiles();

            if (null == files || files.length == 0) {
                System.out.println("文件夹是空的!");
                return;
            } else {
                for (File file2 : files) {
                    //加入替换逻辑
//                    System.out.println(file2.getAbsoluteFile());
                    String filePath = file2.getAbsolutePath();
                    replaceByFile(filePath,dictPath);

                    if (file2.isDirectory()) {
                        System.out.println("文件夹:" + file2.getAbsolutePath());
//                        traverseFolder2(file2.getAbsolutePath());
                        traverseFolder2(filePath, dictPath);
                    } else {
                        System.out.println("文件:" + file2.getAbsolutePath());
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
    }


    public static void findChineseAndWriteLocal(String path, String dictPath, ProcessFile pf) {
        StringBuilder sb = new StringBuilder();

        File file = new File(path);
        if (file.exists()) {
            File[] files = file.listFiles();

            if (null == files || files.length == 0) {
                System.out.println("文件夹是空的!");
                return;
            } else {
                for (File file2 : files) {
                    //加入替换逻辑
//                    System.out.println(file2.getAbsoluteFile());
                    String filePath = file2.getAbsolutePath();
                    //仅对js或jsp文件查找中文
                    if (filePath.endsWith(".js") || filePath.endsWith(".jsp")) {
                        String s = (String) pf.process(filePath, dictPath);
                        sb = sb.append(s);
                        System.out.println("current sb length:" + sb.length());
                    }

                    if (file2.isDirectory()) {
                        System.out.println("文件夹:" + file2.getAbsolutePath());
//                        traverseFolder2(file2.getAbsolutePath());
                        findChineseAndWriteLocal(filePath, dictPath, pf);
                    } else {
                        System.out.println("文件:" + file2.getAbsolutePath());
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }

        String outputPath = "E:\\1\\chinese.txt";
        GetFiles.writeFile(sb.toString(), outputPath, true);
    }





    //抽象方法： 处理文件服务， 需要实现ProcessFile.process方法，再对所有文件进行某种文件操作
    public static void processAllFiles(String path, String dictPath, ProcessFile processFile) {

        File file = new File(path);
        if (file.exists()) {
            File[] files = file.listFiles();

            if (null == files || files.length == 0) {
                System.out.println("文件夹是空的!");
                return;
            } else {
                for (File file2 : files) {
                    //加入替换逻辑
//                    System.out.println(file2.getAbsoluteFile());
                    String filePath = file2.getAbsolutePath();
                    processFile.process(filePath, dictPath);

                    if (file2.isDirectory()) {
                        System.out.println("文件夹:" + file2.getAbsolutePath());
//                        traverseFolder2(file2.getAbsolutePath());
                        processAllFiles(filePath, dictPath, processFile);
                    } else {
                        System.out.println("文件:" + file2.getAbsolutePath());
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
    }





    //字符串匹配正则表达式
    //https://blog.csdn.net/sddh1988/article/details/62891470
    public static Matcher matchRegex(String content, String regex) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(content);
        return matcher;
    }




    //注意:读取格式为UTF-8
    public static String readTxtFile(String filePath) {
        StringBuilder content = new StringBuilder();

        String encoding = "UTF-8";
        try {
            File file = new File(filePath);
            if(file.isFile() && file.exists()) {
                InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                while ( (lineTxt = bufferedReader.readLine()) != null) {
                    content.append(lineTxt + "\r\n");
                }
                read.close();
            }
        } catch (IOException e) {
            System.out.println("文件读取出错");
            e.printStackTrace();
        }
        return content.toString();
    }

    //正常的写入文本——————————
    public static void writeFile(String content, String path) {
        BufferedWriter bw = null;

        try {
            File file = new File(path);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            bw = new BufferedWriter(fw);
            bw.write(content);
            bw.close();
        } catch (IOException e) {
            System.out.println("写入错误！");
            e.printStackTrace();
        }
    }


    //第3个参数，true为追加，false为覆盖
    public static void writeFile(String content, String path, Boolean defaultFalse) {
        BufferedWriter bw = null;

        try {
            File file = new File(path);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file.getAbsoluteFile(), defaultFalse);
            bw = new BufferedWriter(fw);
            bw.write(content);
            bw.close();
        } catch (IOException e) {
            System.out.println("写入错误！");
            e.printStackTrace();
        }
    }



    //获取Properties文件
    public static Properties init(String propsFilePath){
        Properties properties = new Properties();
        File file = new File(propsFilePath);
        try {
            properties.load(new FileInputStream(file));
//            System.out.println("props:"+properties.getProperty("TE"));
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Missing Property file!");
        }
        return properties;
    }

    //一次性读取文件
    public static String loadDict(String dictFilePath) {
        String text = GetFiles.readTxtFile(dictFilePath);
        return text;
    }

//    字符串替换,获取键值对————注意，键值对若有出现过@，可能会有错误
//    public static HashMap<String, String> dictToMap(String dicts) {
////        String dicts = GetFiles.loadDict(dictFilePath);
//        String[] lines = dicts.split("\r\n");
//        HashMap<String, String> map = new HashMap<>();
//        for(String line:lines) {
////            小心此处
//            System.out.println(line);
//            String[] split = line.replaceFirst(",", "@").split("@");
//            System.out.println(split.length);
//            String key = split[0];
//            String value = split[1];
//            map.put(key, value);
//        }
//        return map;
//    }

//    //使用TreeMap
    public static TreeMap<String, String> dictToMap(String dicts) {
//        String dicts = GetFiles.loadDict(dictFilePath);
        String[] lines = dicts.split("\r\n");
        TreeMap<String, String> map = new TreeMap<String, String>();
        for(String line:lines) {
//            小心此处
//            System.out.println(line);
//
//            String[] split = line.replaceFirst(",", "@").split("@");
            //中文键里可能存在“，”, 所以导出时用tab分隔的csv
            String[] split = line.replaceFirst("\t", "@").split("@");
//            System.out.println(split.length);
            String key = split[0];
            String value = split[1];
            map.put(key, value);
        }
        return map;
    }

    public static String[][] dictToTwodArray(String dicts) {
//        String dicts = GetFiles.loadDict(dictFilePath);
        String[] lines = dicts.split("\r\n");
        String splitWith = "\t";
        String[][] twoDarray = new String[lines.length][lines[0].split(splitWith).length];
        int rowLen = lines.length;
        int colLen = lines[0].split(splitWith).length;
//        System.out.println("rowLen is "+ rowLen);
//        System.out.println("colLen is" + colLen);
        for(int i=0; i<rowLen;i++) {
//            小心此处
//            System.out.println(lines[i]);
//
//            String[] split = line.replaceFirst(",", "@").split("@");
            //中文键里可能存在“，”, 所以导出时用tab分隔的csv

//            String[] split = lines[i].replaceFirst(splitWith, "@").split("@");
            //此处逻辑修改10/1 15:31 ——现在获取3列的字符串，将所有tab键替换为@， 长度才会为3
            String[] split = lines[i].replaceAll(splitWith, "@").split("@");
//            System.out.println(split.length);
            for(int j=0; j<colLen;j++) {
                twoDarray[i][j] = split[j];
            }
        }
        return twoDarray;
    }



    //将字符串中的所有键，替换为值——————注意contens可否直接修改
    //应该先替换所有
//    public static String replaceCn( String contents, HashMap<String, String> map) {
    public static String replaceCn( String contents, TreeMap<String, String> map) {
        for( Map.Entry<String, String> entry: map.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            //此处很有可能根本没有替换------------------------------------------------------------------------------------------------
            contents = contents.replaceAll(key, value);
        }

        //新增逻辑，放到List中，List按键的长度降序，先对最长的字符串替换
//        ArrayList<String[]> list1 = new ArrayList<>();
////        map.keySet()
//        Iterator<String> iter = map.keySet().iterator();
//        TreeSet<String> treeSet1 = new TreeSet(new SortByStrLength());
//        while (iter.hasNext()) {
//            treeSet1.add(iter.next());
//            list1.add(new String[]{iter.next(), map.get((String) iter.next())});
//        }
//        for(int i = 0; i<list1.size();i++) {
//            contents = contents.replaceAll(list1.get(i)[0], list1.get(i)[1]);
//        }




        return contents;
    }


    //使用String[][]替换
    public static String replaceCn( String contents, String[][] map) {
        for(int i=0; i<map.length; i++) {
            String key = map[i][0];
            String value = map[i][1];
            //此处很有可能根本没有替换------------------------------------------------------------------------------------------------
            contents = contents.replaceAll(key, value);
        }
        return contents;
    }

//    根据文件后缀名进行替换
    public static String replaceCn( String contents, String[][] map, String filePath) {
        for(int i=0; i<map.length; i++) {
            String key = map[i][0];
            String valueJsp = map[i][1];
            String valueJs = map[i][2];
            if (filePath.endsWith(".jsp")) {
                contents = contents.replaceAll(key, valueJsp);
            } else if (filePath.endsWith(".js")) {
//                contents = contents.replaceAll(key, valueJs);
                //将valueJs前面可能存在 双引号，单引号  的字符串替换掉,使用正则匹配

                String reg = "[\"|'?]"+key+"[\"|'?]";
                Pattern compile = Pattern.compile(reg);
                Matcher matcher1 = compile.matcher(contents);
                while (matcher1.find()) {
                    System.out.println("----------------------------------------------------------------------------------------------");
                    contents = contents.replaceAll("\""+key+"\"", valueJs);
                    contents = contents.replaceAll("'" + key + "'", valueJs);
                }
//                while (contents.matches("\""+valueJs+"\"") || contents.matches("'" + valueJs + "'")) {
//                    contents = contents.replaceAll("\""+valueJs+"\"", valueJs);
//                    contents = contents.replaceAll("'" + valueJs + "'", valueJs);
//                }
            } else {
                System.out.println("file is neither jsp or js format");
            }

        }
        return contents;
    }



//    替换文件操作
    public static void replaceByFile(String filePath, String dictPath) {
//        String path = "C:\\Users\\Administrator\\Desktop\\国际化cl.csv";
        String s = loadDict(dictPath);
//        HashMap<String, String> dictMap = dictToMap(s);
//        TreeMap<String, String> dictMap = dictToMap(s);
        String[][] dictMap = dictToTwodArray(s);
//        @warn 从遍历的文件中获取文件路径
//        String filePath = "E:\\1\\IModel.java";
        String contents = readTxtFile(filePath);
        String newContents = replaceCn(contents, dictMap, filePath);
        writeFile(newContents, filePath);
    }

    //遍历所有文件，获取字符串



    //    替换文件操作
    public static String findGBK(String filePath, String dictPath) {
        String s = loadDict(dictPath);
        String[][] dictMap = dictToTwodArray(s);
        String contents = readTxtFile(filePath);

        StringBuilder sb = new StringBuilder();

        String[] lines = contents.split("\r\n");
        String chineseReg = "[^\u4e00-\u9fa5]";
        for(int row=0; row < lines.length; row++) {

            if( lines[row].matches(chineseReg)) {
                sb.append(row+"\r\n");
            }
        }
        return sb.toString();
    }




//@warn:注意——1：需要将传来的字典csv，先转成”utf-8"格式（伏哥数据库里面建库时，用的应该是gbk)
//2：  数据->A,数据源—>B，  如果先找到“数据”这个键，那“数据源”会被替换成 “B源”,那数据源就替换不正确了
//    考虑字符串的“正则表达式”————或者对键作字符串长度的排序， 先替换长的，再替换短的
//    引号问题： 比如值getString("key221") , 会被替换为 getString("""key221")——————这个可以考虑在replaceCN中把 "" 替换为"
    public static void main(String[] args) {

//
//        String path = "E:\\bonc\\MM1111111111111111\\";
//        String filePath = "E:\\bonc\\MM1111111111111111\\MM\\IModel.java";
//        String outPath = "E:\\1\\IModel12.java";
//        ArrayList<String> fileNames = GetFiles.getFileNames(path);
//        System.out.println(fileNames.size());
////        System.out.println();
//        for (String filename:fileNames) {
//
//            System.out.println("oj"+filename);
//        }
//    }

//        GetFiles getFiles = new GetFiles();
//        getFiles.traverseFolder2(path);

//        String s = GetFiles.readTxtFile(filePath);
//        GetFiles.writeFile(s, outPath);
//        System.out.println(s);

//        Main();
//        String filePath = "E:\\5\\shkjTest\\flow_new\\flow_new";
//        String filePath = "E:\\7\\flow_new\\flow_new";
//      新的jsp页面
//        String filePath = "E:\\8\\pages\\pages";
        String filePath = "E:\\8\\pages";
//        String filePath = "F:\\1\\tmp\\chinese";
        String s = readTxtFile(filePath);
//        System.out.println(s);
//        String filePath = "C:\\Users\\Administrator\\Desktop\\2222222";
        //三列字符串，区分js和jsp
//        String dictPath = "C:\\Users\\Administrator\\Desktop\\tmp\\333.csv";
//        String dictPath = "C:\\Users\\Administrator\\Desktop\\2222222\\国际化cl.csv";
        String dictPath = "C:\\Users\\Administrator\\Desktop\\tmp\\999.csv";
//        替换中文字符
        traverseFolder2(filePath, dictPath);


//        获取所有中文字符
//        ProcessFile pf = new ServiceFindChineseChar();
//        findChineseAndWriteLocal(filePath, dictPath, pf);
    }
}