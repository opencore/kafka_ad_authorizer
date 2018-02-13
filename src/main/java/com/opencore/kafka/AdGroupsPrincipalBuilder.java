package org.apache.kafka.common.security.authenticator;

import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.security.auth.*;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.utils.Java;

import javax.security.sasl.SaslServer;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

public class AdGroupsPrincipalBuilder implements KafkaPrincipalBuilder, Configurable {
    private ShellBasedUnixGroupsMapping groupMapper;
    private DefaultKafkaPrincipalBuilder principalBuilder;

    @Override
    public KafkaPrincipal build(AuthenticationContext context) {
        List<KafkaPrincipal> groupPrincipals  = new ArrayList<>();
        SaslServer saslServer = ((SaslAuthenticationContext) context).server();
        if (context instanceof SaslAuthenticationContext) {
            try {
                List<String> groups = groupMapper.getGroups(principalBuilder.build(context).getName());
                for (String group : groups) {
                    groupPrincipals.add(new KafkaPrincipal("group", group));
                }

            } catch (IOException e) {
                // TODO: log an error that groups could not be resolved
            }
            return new ComplexKafkaPrincipal("adgroupprincipal", saslServer.getAuthorizationID(), groupPrincipals);
        } else {
            return new ComplexKafkaPrincipal("adgroupprincipal", saslServer.getAuthorizationID(), groupPrincipals);
        }
    }


    @Override
    public void configure(Map<String, ?> configs) {
        this.groupMapper = new ShellBasedUnixGroupsMapping();

        // Obtain Kerberos config and shortening rules from config and create a default principal builder that will be
        // used to create the username that we then use to lookup the groups
        this.principalBuilder = new DefaultKafkaPrincipalBuilder(getKerberosShortNamer(configs));
    }

    private KerberosShortNamer getKerberosShortNamer(Map<String, ?> configs) {
        List<String> principalToLocalRules = (List<String>) configs.get(BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG);

        KerberosShortNamer kerberosShortNamer = null;
        if (principalToLocalRules != null) {
            try {
                kerberosShortNamer = KerberosShortNamer.fromUnparsedRules(defaultKerberosRealm(), principalToLocalRules);
            } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                // nothing to do in this case we just move on with a null shortnamer
                // TODO: log a warning
            }
        }
        return kerberosShortNamer;
    }

    private String defaultKerberosRealm() throws ClassNotFoundException, NoSuchMethodException,
            IllegalArgumentException, IllegalAccessException, InvocationTargetException {

        //TODO Find a way to avoid using these proprietary classes as access to Java 9 will block access by default
        //due to the Jigsaw module system
        Object kerbConf;
        Class<?> classRef;
        Method getInstanceMethod;
        Method getDefaultRealmMethod;
        if (Java.isIbmJdk()) {
            classRef = Class.forName("com.ibm.security.krb5.internal.Config");
        } else {
            classRef = Class.forName("sun.security.krb5.Config");
        }
        getInstanceMethod = classRef.getMethod("getInstance", new Class[0]);
        kerbConf = getInstanceMethod.invoke(classRef, new Object[0]);
        getDefaultRealmMethod = classRef.getDeclaredMethod("getDefaultRealm", new Class[0]);
        return (String) getDefaultRealmMethod.invoke(kerbConf, new Object[0]);
    }

}

