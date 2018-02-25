package com.opencore.kafka;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;
import org.apache.kafka.common.security.auth.SslAuthenticationContext;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.utils.Java;
import org.apache.kafka.common.utils.Utils;

public class HadoopGroupMappingPrincipalBuilder implements KafkaPrincipalBuilder, Configurable {
  private Logger principalLogger;
  private GroupMappingServiceProvider groupMapper;
  private DefaultKafkaPrincipalBuilder principalBuilder;
  private String certificateUserField = "CN";

  @Override
  public KafkaPrincipal build(AuthenticationContext context) {
    // Create a base principal by using the DefaultPrincipalBuilder
    ComplexKafkaPrincipal basePrincipal = new ComplexKafkaPrincipal(principalBuilder.build(context));

    // Resolve username based on what kind of AuthenticationContext the request has
    // and perform groups lookup
    if (context instanceof SaslAuthenticationContext) {
      basePrincipal.additionalPrincipals = getGroups(basePrincipal.getName());
    } else if (context instanceof SslAuthenticationContext) {
      basePrincipal.additionalPrincipals = getGroups(getUserFromCertificate(basePrincipal.getName()));
    }
    return basePrincipal;
  }

  private List<KafkaPrincipal> getGroups(String userName) {
    List<KafkaPrincipal> groupPrincipals = new ArrayList<>();
    try {
      // Add user principal to list as well to make later matching easier
      groupPrincipals.add(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, userName));

      principalLogger.fine("Resolving groups for user: " + userName);
      List<String> groups = groupMapper.getGroups(userName);
      principalLogger.fine("Got list of groups for user " + userName + ": " + Utils.join(groups, ", "));
      for (String group : groups) {
        groupPrincipals.add(new KafkaPrincipal("Group", group));
      }
    } catch (IOException e) {
      principalLogger.warning("Groups for user " + userName +
          " could not be resolved, proceeding with authorization based on username only.");
    }
    return groupPrincipals;
  }

  private String getUserFromCertificate(String certificateString) {
    // For a SslContext the username will look like CN=username;OU=...;DN=...
    try {
      LdapName certificateDetails = new LdapName(certificateString);
      for (Rdn currentRdn : certificateDetails.getRdns()) {
        if (currentRdn.getType().equalsIgnoreCase(certificateUserField)) {
          certificateString = currentRdn.getValue().toString();
        }
      }
    } catch (InvalidNameException e) {
      principalLogger.warning("Error extracting username from String " + certificateString + ": " + e.getMessage());
    }
    return certificateString;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    principalLogger = Logger.getLogger("kafka.authorizer.logger");

    // Check if options for the principalbuilder were specified in the broker config
    Map<String, String> authorizerOptions;
    if (configs.containsKey("principal.builder.options")) {
      authorizerOptions = (Map<String, String>) configs.get("principal.builder.options");
    } else {
      authorizerOptions = new HashMap<>();
    }

    if (authorizerOptions.containsKey("mapper.implementation")) {
      // The user specified a mapper class to be used, try to instantiate an object
      Class mapperClass = null;
      try {
        mapperClass = Class.forName(authorizerOptions.get("mapper.implementation"));
      } catch (ClassNotFoundException e) {
        throw new ConfigException("Couldn't instantiate mapper class for principalbuilder: " + e.getMessage());
      }

      if (mapperClass.isAssignableFrom(GroupMappingServiceProvider.class)) {
        // Class implements GroupMappingProvider, we can safely perform this cast
        groupMapper = (GroupMappingServiceProvider) Utils.newInstance(mapperClass);
        // Check if it is configurable
        if (mapperClass.isAssignableFrom(org.apache.hadoop.conf.Configurable.class)) {
          // Class can take a configuration object, we create this with all options
          // taken from
          // TODO: this does not currently do anything as no config can be passed to PrincipalBuilders
          Configuration groupMapperConfig = new Configuration();

          for (String configOption : authorizerOptions.keySet()) {
            // Remove "mapper.configuration" item, as this was intended for this class,
            // not the wrapped mapper implementation
            if (!configOption.equals("mapper.implementation"))
              groupMapperConfig.set(configOption, authorizerOptions.get(configOption));
          }
          ((org.apache.hadoop.conf.Configurable) groupMapper).setConf(groupMapperConfig);
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