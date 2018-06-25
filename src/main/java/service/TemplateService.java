package service;

import common.ConfigException;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;

import java.io.File;

public class TemplateService {
    private final static String tempUrl = "entity.ftl";

    private Template loadTemplate() {
        String tempDir = "/" + tempUrl;
        String path = this.getClass().getResource(tempDir).getPath().replace(tempUrl, "");
        Template template;
        try {
            Configuration config = createConfiguration(new File(path));
            template = config.getTemplate(tempDir);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ConfigException(e.getMessage());
        }
        return template;
    }

    private Configuration createConfiguration(File dir) throws Exception {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
        cfg.setDirectoryForTemplateLoading(dir);
        cfg.setDefaultEncoding("UTF-8");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        return cfg;
    }

    public Template load() {
        return loadTemplate();
    }
}
