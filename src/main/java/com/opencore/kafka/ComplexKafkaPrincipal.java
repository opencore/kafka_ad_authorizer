package com.opencore.kafka;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class ComplexKafkaPrincipal extends KafkaPrincipal {
  protected List<KafkaPrincipal> allPrincipals = new ArrayList<>();

  public ComplexKafkaPrincipal(String principalType, String name) {
    super(principalType, name);
  }

  public ComplexKafkaPrincipal(KafkaPrincipal kafkaPrincipal) {
    this(kafkaPrincipal.getPrincipalType(), kafkaPrincipal.getName());
  }

  public ComplexKafkaPrincipal(String principalType, String name, List<KafkaPrincipal> allPrincipals) {
    this(principalType, name);
    this.allPrincipals = allPrincipals;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ComplexKafkaPrincipal){
      allPrincipals.retainAll(((ComplexKafkaPrincipal)o).allPrincipals);
      return allPrincipals.size() > 0;
    } else if (o instanceof KafkaPrincipal) {
      return allPrincipals.contains(o);
    }
    // we got some Principal that we don't know how to handle
    return false;
  }

  public List<KafkaPrincipal> getPrincipalList() {
    return allPrincipals;
  }
}
