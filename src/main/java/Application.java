import common.ConfigException;
import entity.Columns;
import entity.ConfigProperties;
import freemarker.template.Template;
import model.DataModel;
import service.DataBaseService;
import service.TemplateService;
import util.ConfigUtil;
import util.StringUtil;

import java.io.*;
import java.util.List;

public class Application {
    public void run() {
        OutputStream fos = null;
        Writer out = null;
        try {
            ConfigProperties configProperties = ConfigUtil.init();
            List<Columns> columnsList = new DataBaseService().loadColumnsInfo(configProperties);
            Template template = new TemplateService().load();
            String output = configProperties.getOutput();
            File dir = new File(output);
            if (!dir.isDirectory()) {
                throw new ConfigException("路径不合法");
            }
            if (!dir.exists()) {
                dir.mkdirs();
            }
            fos = new FileOutputStream(new File(dir, StringUtil.underlineToClassName(configProperties.getTableName()) + ".java")); //java文件的生成目录
            out = new OutputStreamWriter(fos);
            template.process(new DataModel(configProperties, columnsList), out);
            System.out.println("SUCCESS!");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("FAILED!");
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (fos != null) {
                    fos.flush();
                    fos.close();
                }
            } catch (IOException ie) {
                //IGNORE
            }
        }
    }

    public static void main(String[] args) {
        new Application().run();
    }
}
