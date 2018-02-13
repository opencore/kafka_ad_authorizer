/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.security.auth

import com.typesafe.scalalogging.Logger
import kafka.common.{NotificationHandler, ZkNodeChangeNotificationListener}
import kafka.network.RequestChannel.Session
import kafka.security.auth.SimpleAclAuthorizer.VersionedAcls
import kafka.server.KafkaConfig
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import kafka.zk.{AclChangeNotificationSequenceZNode, AclChangeNotificationZNode, KafkaZkClient}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{SecurityUtils, Time}

import scala.collection.JavaConverters._
import scala.util.Random

object AdGroupAuthorizer {
        //optional override zookeeper cluster configuration where acls will be stored, if not specified acls will be stored in
        //same zookeeper where all other kafka broker info is stored.
        val ldapServerConfig = "authorizer.ad.server"

        val ZkUrlProp = "authorizer.zookeeper.url"
        val ZkConnectionTimeOutProp = "authorizer.zookeeper.connection.timeout.ms"
        val ZkSessionTimeOutProp = "authorizer.zookeeper.session.timeout.ms"
        val ZkMaxInFlightRequests = "authorizer.zookeeper.max.in.flight.requests"

        //List of users that will be treated as super users and will have access to all the resources for all actions from all hosts, defaults to no super users.
        val SuperUsersProp = "super.users"
        //If set to true when no acls are found for a resource , authorizer allows access to everyone. Defaults to false.
        val AllowEveryoneIfNoAclIsFoundProp = "allow.everyone.if.no.acl.found"

        case class VersionedAcls(acls: Set[Acl], zkVersion: Int)
        }

class AdGroupAuthorizer extends SimpleAclAuthorizer {
  override def authorize(session: Session, operation: Operation, resource: Resource) = super.authorize(session, operation, resource)
}

/*
public class  extends SimpleAclAuthorizer {

    private String adServer = "";

    @Override
    public void configure(Map<String, ?> javaConfigs) {
        super.configure(javaConfigs);
        adServer = (String)javaConfigs.get(ldapServerConfig);
    }

    @Override
    public boolean authorize(RequestChannel.Session session, Operation operation, Resource resource) {
        KafkaPrincipal principal = session.principal();
        String host = session.clientAddress().getHostAddress();


        val acls = getAcls(resource) ++ getAcls(new Resource(resource.resourceType, Resource.WildCardResource))

        // Check if there is any Deny acl match that would disallow this operation.
        val denyMatch = aclMatch(operation, resource, principal, host, Deny, acls)

        // Check if there are any Allow ACLs which would allow this operation.
        // Allowing read, write, delete, or alter implies allowing describe.
        // See #{org.apache.kafka.common.acl.AclOperation} for more details about ACL inheritance.
        val allowOps = operation match {
            case Describe => Set[Operation](Describe, Read, Write, Delete, Alter)
            case DescribeConfigs => Set[Operation](DescribeConfigs, AlterConfigs)
            case _ => Set[Operation](operation)
        }
        val allowMatch = allowOps.exists(operation => aclMatch(operation, resource, principal, host, Allow, acls))

        //we allow an operation if a user is a super user or if no acls are found and user has configured to allow all users
        //when no acls are found or if no deny acls are found and at least one allow acls matches.
        val authorized = isSuperUser(operation, resource, principal, host) ||
                isEmptyAclAndAuthorized(operation, resource, principal, host, acls) ||
                (!denyMatch && allowMatch)

        logAuditMessage(principal, authorized, operation, resource, host)
        authorized
    }
}
*/