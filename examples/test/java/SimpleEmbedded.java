//import org.python.core.PyInteger;
//import org.python.core.PyObject;
//import org.python.util.PythonInterpreter;
//
///**
// * Created by Administrator on 2018/9/26 0026.
// */
//public class JythonDemo1 {
//    public static void main(String[] args) {
//        PythonInterpreter interp = new PythonInterpreter();
//        interp.exec("import sys");
//        interp.exec("print(sys)");
//        interp.set("a", new PyInteger(42));
//        interp.exec("print a");
//        interp.exec("x = 2+2");
//        PyObject x = interp.get("x");
//        System.out.println("x: " + x);
//    }
//}


import org.python.core.PyException;
import org.python.core.PyInteger;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import java.util.Properties;

public class SimpleEmbedded {

    public static void main(String[] args) throws PyException {
//        以下4行为初始化jython必须，否则会报错：
//        Exception in thread "main" ImportError: Cannot import site module and its dependencies: No module named site
//        解决方案见http://bugs.jython.org/issue2355，msg10150 (view)
//        初始化PythonIntepreter
        Properties properties = new Properties();
        properties.put("python.import.site","false");
        Properties preprops = System.getProperties();
        PythonInterpreter.initialize(preprops,properties,new String[0]);
//
        PythonInterpreter interp = new PythonInterpreter();

//        interp.exec("import sys");
//        interp.exec("print sys");
        interp.set("a", new PyInteger(42));
        interp.exec("print a");
        interp.exec("x = 2+2");
        PyObject x = interp.get("x");
        System.out.println("x: " + x);
        interp.execfile("E:\\IdeaWorkSpace\\EnergyManagement4\\EnergyManagement\\python\\python\\MOPSO\\src\\mopso\\main.py");
    }
}