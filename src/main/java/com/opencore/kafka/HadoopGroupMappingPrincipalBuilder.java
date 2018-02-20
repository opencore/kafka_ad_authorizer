package com.opencore.kafka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.security.auth.*;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.utils.Java;

import javax.security.sasl.SaslServer;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import org.apache.kafka.common.utils.Utils;

public class HadoopGroupMappingPrincipalBuilder implements KafkaPrincipalBuilder, Configurable {
  private GroupMappingServiceProvider groupMapper;
  private DefaultKafkaPrincipalBuilder principalBuilder;

  @Override
  public KafkaPrincipal build(AuthenticationContext context) {
    List<KafkaPrincipal> groupPrincipals = new ArrayList<>();

    if (context instanceof SaslAuthenticationContext) {
      SaslServer saslServer = ((SaslAuthenticationContext) context).server();
      try {
        List<String> groups = groupMapper.getGroups(principalBuilder.build(context).getName());
        for (String group : groups) {
          groupPrincipals.add(new KafkaPrincipal("Group", group));
        }

      } catch (IOException e) {
        // TODO: log an error that groups could not be resolved
      }
      return new ComplexKafkaPrincipal(KafkaPrincipal.USER_TYPE, saslServer.getAuthorizationID(), groupPrincipals);
    } else {
      // Nothing to do if we are not in a SASL Context, use the default builder to generate
      // the principal
      return new ComplexKafkaPrincipal(principalBuilder.build(context));
    }
  }


  @Override
  public void configure(Map<String, ?> configs) {
    if (configs.containsKey("principal.builder.groupmapper.class")) {
      Class mapperClass = null;
      try {
        mapperClass = Class.forName((String) configs.get("principal.builder.groupmapper.class"));
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }

      if (mapperClass.isAssignableFrom(GroupMappingServiceProvider.class)) {
        // Class implements GroupMappingProvider, we can safely perform this cast
        groupMapper = (GroupMappingServiceProvider) Utils.newInstance(mapperClass);
        // Check if it is configurable
        if (mapperClass.isAssignableFrom(org.apache.hadoop.conf.Configurable.class)) {
          // Class can take a configuration object, we create this with all options
          // from the Kafka config that have the prefix principal.builder.groupmapper.option
          // by stripping that prefix
          // TODO: this does not currently do anything, as unknown config properties are
          // removed from the config by the broker - needs Kafka change
          Configuration groupMapperConfig = new Configuration();
          for (String configKey : configs.keySet()) {
            if (configKey.startsWith("principal.builder.groupmapper.option.")) {
              String shortKey = configKey.substring(37, configKey.length());
              String value = (String) configs.get(configKey);
              // Add trimmed key and value from Kafka config to config for Hadoop Mapper
              groupMapperConfig.set(shortKey, value);
              ((org.apache.hadoop.conf.Configurable) groupMapper).setConf(groupMapperConfig);
            }
          }
        }
      } else {
        throw new ConfigException("Mapper class must implement org.apache.hadoop.security.GroupMappingServiceProvider");
      }
    } else {
      // Fallback to default of using local groups for lookup
      this.groupMapper = new ShellBasedUnixGroupsMapping();
    }

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