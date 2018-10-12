import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ObjectUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2018/10/1 0001.
 */
public class test1 {

    public static void main(String[] args) {
//        String reg = "[^\u4e00-\u9fa5]";
//        String ch = "我的天";
//        Pattern pattern = Pattern.compile(reg);
//        boolean matches = ch.matches(reg);
//        System.out.println(matches);
//        Matcher matcher = pattern.matcher(ch);

//        String contents = "asd '中文' 123 \"中文\" &(*^*^8";
//        String key = "中文";
////        String reg = "'hk";
//        String reg = "[\"|']"+key+"[\"|']";
//        String valueJs = "i18nBundle.getString(\"i18n.225\")";
//        String ch = "'jhkjjffk";
//
//        Pattern compile = Pattern.compile(reg);
//        Matcher matcher1 = compile.matcher(contents);
//        while (matcher1.find()) {
//            contents = contents.replaceAll("\""+key+"\"", valueJs);
//            contents = contents.replaceAll("'" + key + "'", valueJs);
//        }
//        System.out.println(contents);

//        Pattern pattern = Pattern.compile(reg);
//        Matcher matcher = pattern.matcher(ch);
//        while (matcher.find()) {
//            System.out.println("true");
//        }

        String path = "E:\\8\\pages\\pages\\dm\\datasource\\connectionManager.jsp";
        String text = GetFiles.readTxtFile(path);

//        String result = text.replaceAll("/\\*.*\\*/", "");
//
//        StringBuilder sb = new StringBuilder();
//        String[] split = result.split("\r\n");
//        for(int i=0; i<split.length; i++) {
//            String filter = split[i].replaceAll("//.*", "");
//            sb.append(filter + "\r\n");
//        }
//        String s = deleleComment(text);
//        System.out.println(text);

//        String s = "hfhfhfh//我的的划分非法，我的>fjfj我的复合合法，的djfjfj";
//        String s = "hfhfhfh//我的的划分非法，,我的>fjfj我的复合合法，的djfjfj";
//        String s1 = s.replaceAll("[a-zA-Z]+", "@");
//        System.out.println(s1);
        StringBuilder sb = new StringBuilder();
        String s = "fjjff:\"我的天\",fj\"多环境\",ff\'王府井\'";
        if (s.indexOf("\"") != -1) {
            String[] split = s.split("[\"|\'|<|>]");
            for(int i = 0;i<split.length;i++) {
                if (Pattern.compile("[\\u4e00-\\u9fa5]").matcher(split[i]).find()) {
                    sb.append(split[i]+"@");
                }
            }
        }
        System.out.println(sb.toString());
//        System.out.println(deleleComment(text));
    }

    public static String extractChineseFromLine(String line, StringBuilder sb) {
        String[] split1 = line.split("[\"|\'|<|>]");
        for(int i = 0;i<split1.length;i++) {
            if (Pattern.compile("[\\u4e00-\\u9fa5]").matcher(split1[i]).find()) {
                sb.append(split1[i]+"@");
            }
        }
        return sb.toString();
    }




    public static String deleleComment(String context) {

        StringBuilder sb = new StringBuilder();
//        替换掉java的多行及文档注释, 这貌似只能替换单行的
//        String result = context.replaceAll("/\\*.*\\*/", "");
        String result = context.replaceAll("/\\*(.|[\r\n])*?\\*/", "");
        //删除一至多行注释，可以跨行，贪婪模式
        String result1 = result.replaceAll("<!--(.|[\r\n])*?-->","");
//        System.out.println(result1);
        String[] split = result1.split("\r\n");
        for(int i=0; i<split.length; i++) {
            String filter = split[i].replaceAll("//.*", "");
            sb.append(filter + "\r\n");
        }
        return sb.toString();
    }

    public static String deleteJavaComment(String s) {
        Pattern pattern1 = Pattern.compile("//(.*)"); //特征是所有以双斜线开头的
        Matcher matcher1 = pattern1.matcher(s);
        s = matcher1.replaceAll(""); //替换第一种注释

        Pattern pattern2 = Pattern.compile("/\\*(.*?)\\*/", Pattern.DOTALL); //特征是以/*开始，以*/结尾，Pattern.DOTALL的意思是糊涂模式，这种模式下.（点号）匹配所有字符
        Matcher matcher2 = pattern2.matcher(s);
        s = matcher2.replaceAll(""); //替换第二种注释
        Pattern pattern3 = Pattern.compile("/\\*\\*(.*?)\\*/", Pattern.DOTALL); //特征是以/**开始，以*/结尾
        Matcher matcher3 = pattern3.matcher(s);
        s = matcher3.replaceAll(""); //替换第三种注释

        Pattern[] patterns = {pattern1,pattern2,pattern3};
//        for(int i = 0; i< list.size(); i++) {
//
//        }

        System.out.println(s); //打印结果
        return "f";
    }

}


//删除html注释:http://zhangbo-peipei-163-com.iteye.com/blog/2039663
//class HtmlCommentHandler {
//    /**
//     * html内容中注释的Detector
//     *
//     * @author boyce
//     * @version 2013-12-3
//     */
//    private static class HtmlCommentDetector {
//
//        private static final String COMMENT_START = "<!--";
//        private static final String COMMENT_END = "-->";
//
//        // 该字符串是否是html注释行，包含注释的开始标签且结束标签"<!-- -->"
//        private static boolean isCommentLine(String line) {
//
//            return containsCommentStartTag(line) && containsCommentEndTag(line)
//                    && line.indexOf(COMMENT_START) < line.indexOf(COMMENT_END);
//        }
//
//        // 是否包含注释的开始标签
//        private static boolean containsCommentStartTag(String line) {
//            return StringUtils.isNotEmpty(line) &&
//                    line.indexOf(COMMENT_START) != -1;
//        }
//
//        // 是否包含注释的结束标签
//        private static boolean containsCommentEndTag(String line) {
//            return StringUtils.isNotEmpty(line) &&
//                    line.indexOf(COMMENT_END) != -1;
//        }
//
//        /**
//         * 删除该行中的注释部分
//         */
//        private static String deleteCommentInLine(String line) {
//
//            while (isCommentLine(line)) {
//                int start = line.indexOf(COMMENT_START) + COMMENT_START.length();
//                int end = line.indexOf(COMMENT_END);
//                line = line.substring(start, end);
//            }
//            return line;
//        }
//
//        // 获取开始注释符号之前的内容
//        private static String getBeforeCommentContent(String line) {
//            if (!containsCommentStartTag(line))
//                return line;
//
//            return line.substring(0, line.indexOf(COMMENT_START));
//        }
//
//        // 获取结束注释行之后的内容
//        private static String getAfterCommentContent(String line) {
//            if (!containsCommentEndTag(line))
//                return line;
//
//            return line.substring(line.indexOf(COMMENT_END) + COMMENT_END.length());
//        }
//    }
//
//    /**
//     * 读取html内容，去掉注释
//     */
//    public static String readHtmlContentWithoutComment(BufferedReader reader) throws IOException {
//        StringBuilder builder = new StringBuilder();
//        String line = null;
//
//        // 当前行是否在注释中
//        boolean inComment = false;
//        while (ObjectUtils.anyNotNull(line = reader.readLine())) {
//
//            // 如果包含注释标签
//            while (HtmlCommentDetector.containsCommentStartTag(line) ||
//                    HtmlCommentDetector.containsCommentEndTag(line)) {
//
//                // 将成对出现的注释标签之间的内容删除
//                // <!-- comment -->
//                if (HtmlCommentDetector.isCommentLine(line)) {
//                    line = HtmlCommentDetector.deleteCommentInLine(line);
//                }
//
//                // 如果不是注释行，但是依然存在开始标签和结束标签，结束标签一定在开始标签之前
//                // xxx -->content<!--
//                else if (HtmlCommentDetector.containsCommentStartTag(line) && HtmlCommentDetector.containsCommentEndTag(line)) {
//                    // 获取结束标签之后，开始标签之前的文本，并且将 inComment设置为true
//                    line = HtmlCommentDetector.getAfterCommentContent(line);
//                    line = HtmlCommentDetector.getBeforeCommentContent(line);
//                    inComment = true;
//                }
//
//                // 如果只存在开始标签，因为注释标签不支持嵌套，只有开始标签的行一定不会inComment
//                // content <!--
//                else if (!inComment && HtmlCommentDetector.containsCommentStartTag(line)) {
//                    // 将 inComment 设置为true。获取开始标签之前的内容
//                    inComment = true;
//                    line = HtmlCommentDetector.getBeforeCommentContent(line);
//                }
//
//                // 如果只存在结束标签，因为注释标签不支持嵌套，只有结束标签的行一定inComment
//                // -->content
//                else if (inComment && HtmlCommentDetector.containsCommentEndTag(line)) {
//                    // 将 inComment 设置为false。获取结束标签之后的内容
//                    inComment = false;
//                    line = HtmlCommentDetector.getAfterCommentContent(line);
//                }
//
//                // 保存该行非注释的内容
//                if (StringUtils.isNotEmpty(line))
//                    builder.append(line);
//            }
//
//            // 保存该行不存在任何注释标签的并且inComment = false的行
//            if (StringUtils.isNotEmpty(line) && !inComment)
//                builder.append(line);
//        }
//        return builder.toString();
//    }
//}