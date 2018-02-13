package com.opencore.kafka;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class ComplexKafkaPrincipal extends KafkaPrincipal{

    protected List<KafkaPrincipal> additionalPrincipals = new ArrayList<>();
    public ComplexKafkaPrincipal(String principalType, String name) {
        super(principalType, name);
    }

    public ComplexKafkaPrincipal(String principalType, String name, boolean tokenauth) {
        super(principalType, name, tokenauth);
    }

    public ComplexKafkaPrincipal(String principalType, String name, List<KafkaPrincipal> additionalPrincipals) {
        this(principalType, name);
        this.additionalPrincipals = additionalPrincipals;
    }

    public List<KafkaPrincipal> getGroupMemberships() {
        return additionalPrincipals;
    }
}
