/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.core;

import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.core.cluster.MemberChangeListener;
import com.alibaba.nacos.core.cluster.MemberUtil;
import com.alibaba.nacos.core.cluster.MembersChangeEvent;
import com.alibaba.nacos.core.cluster.NodeState;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.sys.env.EnvUtil;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * 判断哪个服务器响应输入
 * Distro mapper, judge which server response input service.
 *
 * @author nkorange
 */
@Component("distroMapper")
public class DistroMapper extends MemberChangeListener {

    /**
     * 健康节点列表，需要保证，全部节点的顺序一致
     * List of service nodes, you must ensure that the order of healthyList is the same for all nodes.
     */
    private volatile List<String> healthyList = new ArrayList<>();

    private final SwitchDomain switchDomain;

    private final ServerMemberManager memberManager;

    public DistroMapper(ServerMemberManager memberManager, SwitchDomain switchDomain) {
        this.memberManager = memberManager;
        this.switchDomain = switchDomain;
    }

    public List<String> getHealthyList() {
        return healthyList;
    }

    /**
     * 初始化服务器列表
     * init server list.
     */
    @PostConstruct
    public void init() {
        // 注册消息订阅
        NotifyCenter.registerSubscriber(this);
        this.healthyList = MemberUtil.simpleMembers(memberManager.allMembers());
    }


    public boolean responsible(Cluster cluster, Instance instance) {
        return
                // 开启健康检查
                switchDomain.isHealthCheckEnabled(cluster.getServiceName())
                        //  集群 健康检查任务 未取消
                        && !cluster.getHealthCheckTask().isCancelled() && responsible(cluster.getServiceName())
                        // 集群包含实例
                        && cluster.contains(instance);
    }

    /**
     * 判断当前服务器是否负责
     * Judge whether current server is responsible for input tag.
     *
     * @param responsibleTag responsible tag, serviceName for v1 and ip:port for v2 | v1版本是服务名词，v2版本是ip:port
     * @return true if input service is response, otherwise false  | 输入服务是响应
     */
    public boolean responsible(String responsibleTag) {
        // 全部节点（包含自己）
        final List<String> servers = healthyList;

        if ( // 没有开启distro
                !switchDomain.isDistroEnabled() || // 或者
                        // 单机模式（单机模式直接负责，因为那是一定给自己的请求，没有别人了）
                        EnvUtil.getStandaloneMode()) {

            return true;
        }

        if (CollectionUtils.isEmpty(servers)) {
            // distro配置没准备好
            // means distro config is not ready yet
            return false;
        }

        // TODO 为什么我没在集群中？
        // 获取本机ip
        String localAddress = EnvUtil.getLocalAddress();
        // 获取本机在服务列表中的位置（第一次位置）
        int index = servers.indexOf(localAddress);
        // 获取本机在服务列表中的位置（最后一次位置）
        int lastIndex = servers.lastIndexOf(localAddress);
        if (lastIndex < 0 || index < 0) {
            // 如果 都不存在，就是负责
            return true;
        }

        // 否则？？
        int target = distroHash(responsibleTag) % servers.size();
        return target >= index && target <= lastIndex;
    }

    /**
     * Calculate which other server response input tag.
     *
     * @param responsibleTag responsible tag, serviceName for v1 and ip:port for v2
     * @return server which response input service
     */
    public String mapSrv(String responsibleTag) {
        final List<String> servers = healthyList;

        if (CollectionUtils.isEmpty(servers) || !switchDomain.isDistroEnabled()) {
            return EnvUtil.getLocalAddress();
        }

        try {
            int index = distroHash(responsibleTag) % servers.size();
            return servers.get(index);
        } catch (Throwable e) {
            Loggers.SRV_LOG.warn("[NACOS-DISTRO] distro mapper failed, return localhost: " + EnvUtil.getLocalAddress(), e);
            return EnvUtil.getLocalAddress();
        }
    }

    private int distroHash(String responsibleTag) {
        return Math.abs(responsibleTag.hashCode() % Integer.MAX_VALUE);
    }

    @Override
    public void onEvent(MembersChangeEvent event) {
        // Here, the node list must be sorted to ensure that all nacos-server's
        // node list is in the same order
        List<String> list = MemberUtil.simpleMembers(MemberUtil.selectTargetMembers(
                // 拿到的是全部成员（只改了状态，并不一定会删除下线的）
                event.getMembers(),
                // 只返回正常和疑似下线的节点
                member -> NodeState.UP.equals(member.getState()) || NodeState.SUSPICIOUS.equals(member.getState())));
        // 进行排序，因为所有集群节点顺序要一样
        Collections.sort(list);
        Collection<String> old = healthyList;
        healthyList = Collections.unmodifiableList(list);
        Loggers.SRV_LOG.info("[NACOS-DISTRO] healthy server list changed, old: {}, new: {}", old, healthyList);
    }

    @Override
    public boolean ignoreExpireEvent() {
        return true;
    }
}
