<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
   http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
  
# Role History: how Hoya brings back nodes in the same location

### 2013-09-20: WiP design doc

## Introduction

Hoya tries to bring up instances of a given role on the machine(s) on which
they last ran -it remembers after flexing or cluster freeze which
servers were last used for a role, and persists this for use in restarts.

It does this in the assumption that the role instances prefer node-local
access to data previously persisted to HDFS. This is precisely the case
for Apache HBase, which can use Unix Domain Sockets to talk to the DataNode
without using the TCP stack. The HBase master persists to HDFS the tables
assigned to specific Region Servers, and when HBase is restarted its master
tries to reassign the same tables back to Region Servers on the same machine.

For this to work in a dynamic cluster, Hoya needs to bring up Region Servers
on the previously used hosts, so that the HBase Master can re-assign the same
tables.

### Assumptions

Here are some assumptions in Hoya's design


1. Instances of a specific role should preferably be deployed onto different
servers. This enables Hoya to only remember the set of server nodes onto
which instances were created, rather than more complex facts such as "two Region
Servers were previously running on Node #17. On restart Hoya can simply request
one instance of a Region Server on a specific node, leaving the other instance
to be arbitrarily deployed by YARN. This strategy should help reduce the *affinity*
in the role deployment, so increase their resilience to failure.


1. There is no need to make sophisticated choices on which nodes to request
re-assignment -such as recording the amount of data persisted by a previous
instance and prioritising nodes based on such data. More succinctly 'the
only priority needed when asking for nodes is *ask for the most recently used*.

1. Different roles are independent: it is not an issue if a role of one type
 (example, an Accumulo Monitor and an Accumulo Tablet Server) are on the same
 host. This assumption allows Hoya to only worry about affinity issues within
 a specific role, rather than across all roles.
 
1. After a cluster has been started, the rate of change of the cluster is
low: both node failures and cluster flexing happen at the rate of every few
hours, rather than every few seconds. This allows Hoya to avoid needing
data structures and layout persistence code designed for regular and repeated changes.

1. instance placement is best effort: if the previous placement cannot be satisfied,
the application will still perform adequately with role instances deployed
onto new servers. More specifically, if a previous server is unavailable
for hosting a role instance due to lack of capacity or availability, Hoya
will not decrement the number of instances to deploy: instead it will rely
on YARN to locate a new node -ideally on the same rack.

1. If two instances of the same role do get assigned to the same server, it
is not a failure condition.

### The Role History

The RoleHistory is a datastructure which models the role assignment, and
can persist it to and restore it from the (shared) filesystem.

* For each role, there is a list of nodes used in the past
* This history is used when selecting a node for a role
* This history remembers when nodes were allocated. These are re-requested
when thawing a cluster.
* It must also remember when nodes were released -these are re-requested
when returning the cluster size to a previous size during flex operations.
* It has to track nodes for which Hoya has an outstanding container request
with YARN. This ensures that the same node is not requested more than once
due to outstanding requests.
* It does not retain a complete history of the role -and does not need to.
All it needs to retain is the recent history for every node onto which a role
instance has been deployed. Specifically, the last allocation or release
operation on a a nodes is all that needs to be persisted. 

* By storing the history of node operations in a time-ordered sequence,
determining which nodes to request is trivial: the nodes at the front
of the list which are not currently running work or onto which new work has been
requested.

* On AM startup, all nodes are considered candidates, even those nodes marked
as active -as they were from the previous instance.
* During cluster flexing, nodes marked as released -and for which there is no
outstanding request - are considered candidates for requesting new instances.
* The AM picks the candidate nodes from the head of the time-ordered list.

### Persistence

The state of the role is persisted to HDFS on changes

1. When nodes are allocated, the Role History is marked as dirty
1. When nodes are released, the Role History is marked as diry
1. When nodes are requested, the Role History is *not* marked as dirty. This
information is not relevant on AM restart.

As at startup a large number of allocations may arrive in a short period of time,
the Role History may be updated very rapidly -yet as the containers are
only recently activated, it is not likely that an immediately restarted Hoya
cluster would gain by re-requesting containers on them -their historical
value is more important than their immediate past.

Accordingly, the role history may be persisted to HDFS asynchronously, with
the dirty bit triggering an flushing of the state to HDFS. The datastructure
will still need to be synchronized for cross thread access, but the 
sync operation will not be a major deadlock, compared to saving the file on every
container allocation response (which will actually be the initial implementation).


## Weaknesses in this design

**Blacklisting**: even if a node fails repeatedly, this design will still try to re-request
instances on this node: there is no blacklisting. As a central blacklist
for YARN has been proposed, it is hoped that this issue will be addressed centrally,
without Hoya having to remember which nodes are unreliable *for that particular
Hoya cluster*.

**Anti-affinity**: Anti-affinity is automatic with a design that only stores
information about single nodes, not about multiple containers in a single node
at the same time. If multiple role instances have been assigned to a single
node in the past, only one of them may end up being local to its
data on a cluster restart. That is, Hoya is prioritising anti-affinity
over performance. 

**Bias towards recent nodes over most-used**: re-requesting the most
recent nodes, rather than those with the most history of use, may
push Hoya to requesting nodes that were only briefly in use -and so have
on a small amount of local state, over nodes that have had long-lived instances.
This is a problem that could perhaps be addressed by preserving more
history of a node -maintaining some kind of moving average of
node use and picking the heaviest used, or some other more-complex algorithm.
This may be possible, but we'd need evidence that the problem existed before
trying to address it.

## Data Structures



That is: for every role, there's a list, RoleList of Node Events.

A Node Event


    Class NodeEvent:
     String node;
     Int: event
     int: timestamp
```

the event enumeration would have the following values

    1: added
    2: released
    3: requested
    4. release-requested
    
```

The main operations on a RoleList are



    contains_event(RL, node) -> true iff exits element L in RL where L.node == node
    
    remove_all_events(RL, node) -> RL' 
     where RL' is all elements L in RL where L.node != node
    
    
    add(RL, node) ->
      RL' = [added(node) | remove_all_events(RL, node)]
      
    release(RL, node) -> 
      RL' = [released(node) | remove_all_events(RL, node)]
    
    request(RL, node) -> 
      RL' = [requested(node) | RL]
    
    // startup picks first count nodes
    findFreeNodesInStartup(RL, count) :-
      [head | tail] = RL,
      [head | findFreeNodesInStartup(tail, count -1).
      
    //at flex time, find first deleted node that hasn't got a request in the queue
    //ahead of it
    
    findFreeFlexNode(RL, [])
    
    findFreeFlexNode([], L) = null;

    //add released nodes to the list of released nodes
    
    
    findFreeFlexNode([released(N) | T ], L) =
     findFreeFlexNode([T], [N, L]).
     
    //added nodes are skipped
    findFreeFlexNode([added(N) | T ], L) =
     findFreeFlexNode([T], L).
     
    //a released node is returned 
    findFreeFlexNode([released(N) | T ], L) =
     if (L.contains(N)) then
       findFreeFlexNode([T], L)
     else
       N.
       
    requestNode(RL) ->
     N = findFreeFlexNode(RL),
     if (N==null) then (null, RL) else (N, add(RL, N))
     
    requestNodes(RL, 0) = ([], RL)
    requestNodes(RL, n) =
      (N, RL') = requestNode(RL);
      if (N==null) then 
        ([], RL)
      else 
        (NL, RL'') = requestNodes(RL', n-1);
        ([N|NL], RL'').
     


The aggregate RoleHistory consists of


    rolemap: Map&lt;String, RoleList&gt;
    dirty: true

    
    get(RH, role) =
      if (rolemap(role)!=null) then
        (rolemap(role), RH)
      else
        RH' = RH where rolemap(role)=[],
        (rolemap(role), RH')
    
    



## AM History Manipulation Actions


### First Boot

1. Attempt to read `RoleHistory` from file fails
1. An empty `RoleHistory` object is created
1. The cluster is flexed
 
 
### Thaw

1. Attempt to read `RoleHistory`. Failure => First Boot.
1. RoleHistory is loaded from file.
1. For each RoleList in RoleHistory: a new RoleList is built up in which every
role listed as allocated is now listed as released.
1. The cluster is flexed
 
### AM Restart

Here the AM of a live cluster is restarted. 

* The RM must notify the AM that this is a restart, and provide a list of containers,
including details on which node they are on.
* Other data structures will be built up modeling the live state of the cluster;
these data structures support the fact that a live cluster can have multiple
containers for the same role hosted on the same server.

1. Attempt to read `RoleHistory`. Failure => First Boot.
1. RoleHistory is loaded from file.
1. For each RoleList in RoleHistory: a new RoleList is built up in which every
role listed as allocated is now listed as released.
1. For every live container(C), the role list of role(container), add(node(container)).
1. The cluster is flexed


Step 4 ensures that every node on which one more more live containers exists
is marked as in use.


### Flex: node addition

For each role that is having more instances requested
1. `R = requestNodes(RL, n)` is used to build a list of nodes to be requested.
1. For each *r* in *R*, a request is made for a new instance,  with the location data
1. For `N=length(R)..n`: request a node without adding any location restrictions.

All the requests can be combined into a single request with a list of resources.

### Flex: node removal

Here one or more nodes are removed. It is essential to not immediately mark
a node as released when a container with that node in is released -it should
only be released when the last container on a node is released.

1. A node to be released is chosen.
1. If it is the sole instance in that role listed as assigned to that node
*and not itself in a pending-release state*,
a release-request event is added to the role history.



### Node assignment callback from RM

* If the node is marked as requested or deleted it is now marked as allocated.
* If the node is marked as active, it is left alone (probable multiple instances)




### Node failure callback from RM

1. A container-release operation is made on the container map; this may mark
a node in the node history as released.
1. the cluster is flexed.

There's another strategy here: request new container is requested on the same
node that failed. 

#### Strengths

* This would support a hot-restart, where all the data is local and hot.
* If the process has just failed, there's likely to be a free container there.
* It is unlikely to make affinity any worse.

#### weaknesses

* It wouldn't do anything to reduce anti-affinity
* If the node itself has failed, the request won't be satisifed.

On the basis that this strategy shouldn't make affinity any worse, and is likely
to benefit performance, the algorithm becomes

* If the container instance was in a pending-release state, mark it as released
 (i.e. pass on to 'container release callback'

* Immediately request a new container on this node with lax placement requirements.
* Insert a request event into the role history


### Container release callback from RM

THis is the callback when a requested-release container is successfully released.

1. If this is the sole node in the live container dataset assigned to this
node, * and there is no acquire-request against it*,  a released() event is added to the history.
1. If there are other containers on this node, 




