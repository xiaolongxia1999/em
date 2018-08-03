package com.bonc.JavaUtils;

import com.bonc.Interface.IModel;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by Administrator on 2018/7/23 0023.
 */
public class ModelsFactory {

    public static Properties init(String propsFilePath){
        Properties properties = new Properties();
        File file = new File(propsFilePath);
        try {
            properties.load(new FileInputStream(file));
            System.out.println("props:"+properties.getProperty("TE"));
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Missing Property file!");
        }
        return properties;
    }

    public static IModel produce(String shortName,Properties props){
        IModel model = null;
        try {
            String modelName = props.getProperty(shortName);
            model = (IModel) Class.forName(modelName).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return model;
    }

    public static void main(String[] args) {
        String path = "conf/default.properties";
        Properties props = ModelsFactory.init(path);
        IModel te1 = ModelsFactory.produce("TE", props);
        System.out.println("jdj");
//        te1.setParam("s");


    }

}
