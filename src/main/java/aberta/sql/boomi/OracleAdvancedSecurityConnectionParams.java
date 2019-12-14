package aberta.sql.boomi;

import aberta.sql.SimpleSQL;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class OracleAdvancedSecurityConnectionParams implements
        SimpleSQL.ConnectionParameters {

    private final Method getDynamicProcessProperty;
    private Method getProperties = null;
    private Constructor gString = null;
    private Object dataContext = null;
    private int documentNumber = 0;
    
    private String algorithm;
    private String checksum;

    public OracleAdvancedSecurityConnectionParams() {
        try {
            getDynamicProcessProperty = Class.forName(
                    "com.boomi.execution.ExecutionUtil")
                    .getDeclaredMethod(
                            "getDynamicProcessProperty", java.lang.String.class);

            algorithm = null;
            checksum = null;
            
        } catch (ClassNotFoundException | NoSuchMethodException |
                 SecurityException ex) {
            throw new RuntimeException(
                    "Cannot obtain getDynamicProcessProperty method", ex);
        }
        
        try {
            gString = Class.forName("org.codehaus.groovy.runtime.GStringImpl")
                    .getConstructor(Object[].class, String[].class);
        } catch (ClassNotFoundException | NoSuchMethodException | SecurityException ex) {
            throw new RuntimeException(
                    "Cannot obtain GStringImpl constructor", ex);
        }
    }
    
    public OracleAdvancedSecurityConnectionParams withEncryptionAlgorithm(String algorithm) {
        this.algorithm = algorithm;
        return this;
    }
    public OracleAdvancedSecurityConnectionParams withChecksumAlgorithm(String algorithm) {
        this.checksum = algorithm;
        return this;
    }
        
    public OracleAdvancedSecurityConnectionParams withDataContext(Object dc) {
        try {

            dataContext = dc;
            documentNumber = 0;
            
            if (dataContext != null) {
                getProperties = dataContext.getClass().getDeclaredMethod(
                        "getProperties", Integer.class);
            } else {
                getProperties = null;
            }

            return this;
            
        } catch (NoSuchMethodException | SecurityException ex) {
            throw new RuntimeException(
                    "Cannot obtain get getProperties method", ex);
        }
    }

    public OracleAdvancedSecurityConnectionParams withDocumentNumber(int num) {
        documentNumber = num;
        return this;
    }
 
    public String getDynamicProperty(String name, String defaultValue) {
        String p = getDynamicProperty(name);
        if (p == null || p.isEmpty()) {
            p = defaultValue;
        }
        return (p != null)? p: "";
    }
    public String getDynamicProperty(String name) {
        String p = getDynamicDocumentProperty(name);
        if (p.isEmpty()) {
            p = getDynamicProcessProperty(name);
        }
        return p;
    }

    public String getDynamicDocumentProperty(String name) {

        if (dataContext == null || name == null || name.trim().isEmpty()) {
            return "";
        }
        if (!name.startsWith("DDP_")) {
            name = "DDP_" + name;
        }
        name = "document.dynamic.userdefined." + name;

        try {
            Properties props = (Properties) getProperties.invoke(dataContext,
                                                                 name);
            return props.getProperty(name, "");
        } catch (IllegalAccessException | IllegalArgumentException |
                 InvocationTargetException ex) {
            throw new RuntimeException(
                    "Failed to get dynamic document property " + name, ex);
        }
    }

    public String getDynamicProcessProperty(String name) {
        if (name == null || name.trim().isEmpty()) {
            return "";
        }
        if (!name.startsWith("DPP_")) {
            name = "DPP_" + name;
        }
        Object obj;
        try {
            obj = getDynamicProcessProperty.invoke(null, name);
        } catch (IllegalAccessException | IllegalArgumentException |
                 InvocationTargetException ex) {
            Logger.getLogger(OracleAdvancedSecurityConnectionParams.class.
                    getName()).
                    log(Level.SEVERE, null, ex);
            throw new RuntimeException(
                    "Failed to get dynamic process property " + name, ex);
        }

        return (obj == null) ? "" : obj.toString();
    }

    @Override
    public String getDriverClass() {
        return getDynamicProperty("JDBC_DRIVER");
    }

    @Override
    public String getConnectionString() {
        return getDynamicProperty("JDBC_URL");
    }

    @Override
    public String getUser() {
        return getDynamicProperty("JDBC_USER");
    }

    @Override
    public String getPassword() {
        return getDynamicProperty("JDBC_PASSWORD");
    }

    @Override
    public Properties getProperties() {
        
        Properties p = new Properties();
        if (algorithm == null || algorithm.isEmpty()) {
            algorithm = getDynamicProperty("JDBC_AOS_ENCRYPTION", "AES256");
        }
        if (checksum == null || checksum.isEmpty()) {
            checksum = getDynamicProperty("JDBC_AOS_CHECKSUM", "MD5");            
        }
        
        p.put("oracle.net.encryption_client", newGString("ACCEPTED"));
        p.put("oracle.net.encryption_types_client", newGString("( " + algorithm + " )"));
        p.put("oracle.net.crypto_checksum_client", newGString("ACCEPTED"));
        p.put("oracle.net.crypto_checksum_types_client", newGString("( " + checksum + " )"));

        return p;
    }    
    
    private Object newGString(String obj) {
        try {
            return gString.newInstance(new Object[] {}, new String[] { obj });
        } catch (InstantiationException | IllegalAccessException |
                IllegalArgumentException |
                InvocationTargetException ex) {
            throw new RuntimeException("Failed to create new GString", ex);
        }
    }
}
