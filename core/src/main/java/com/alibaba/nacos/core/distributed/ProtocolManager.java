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

package com.alibaba.nacos.core.distributed;

import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.consistency.Config;
import com.alibaba.nacos.consistency.ap.APProtocol;
import com.alibaba.nacos.consistency.cp.CPProtocol;
import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.MemberChangeListener;
import com.alibaba.nacos.core.cluster.MemberMetaDataConstants;
import com.alibaba.nacos.core.cluster.MemberUtil;
import com.alibaba.nacos.core.cluster.MembersChangeEvent;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.utils.ClassUtils;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * 一致性协议管理，负责管理Nacos种一致性协议的生命周期
 * Conformance protocol management, responsible for managing the lifecycle of conformance protocols in Nacos.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
@SuppressWarnings("all")
@Component(value = "ProtocolManager")
public class ProtocolManager extends MemberChangeListener implements DisposableBean {
    
    private CPProtocol cpProtocol;
    
    private APProtocol apProtocol;
    
    private final ServerMemberManager memberManager;
    
    private boolean apInit = false;
    
    private boolean cpInit = false;
    
    private Set<Member> oldMembers;
    
    public ProtocolManager(ServerMemberManager memberManager) {
        this.memberManager = memberManager;
        NotifyCenter.registerSubscriber(this);
    }

    /**
     * 将成员转换为 AP 协议
     */
    public static Set<String> toAPMembersInfo(Collection<Member> members) {
        Set<String> nodes = new HashSet<>();
        members.forEach(member -> nodes.add(member.getAddress()));
        return nodes;
    }

    /**
     * 将成员转换为 CP 协议
     */
    public static Set<String> toCPMembersInfo(Collection<Member> members) {
        Set<String> nodes = new HashSet<>();
        members.forEach(member -> {
            // 获取ip
            final String ip = member.getIp();
            // 获取raft端口
            final int raftPort = MemberUtil.calculateRaftPort(member);
            nodes.add(ip + ":" + raftPort);
        });
        return nodes;
    }

    /**
     * 获取 CP 协议
     * @return
     */
    public CPProtocol getCpProtocol() {
        synchronized (this) {
            if (!cpInit) {
                // 没有初始化，就初始化一下
                initCPProtocol();
                cpInit = true;
            }
        }
        return cpProtocol;
    }

    /**
     * 获取 AP 协议
     * @return
     */
    public APProtocol getApProtocol() {
        synchronized (this) {
            if (!apInit) {
                // 没有初始化，就初始化一下
                initAPProtocol();
                apInit = true;
            }
        }
        return apProtocol;
    }

    /**
     * 容器关闭
     */
    @PreDestroy
    @Override
    public void destroy() {
        if (Objects.nonNull(apProtocol)) {
            apProtocol.shutdown();
        }
        if (Objects.nonNull(cpProtocol)) {
            cpProtocol.shutdown();
        }
    }

    /**
     * 初始化 AP 协议
     */
    private void initAPProtocol() {
        ApplicationUtils.getBeanIfExist(APProtocol.class, protocol -> {
            // AP 协议存在，进来
            // 配置类型
            Class configType = ClassUtils.resolveGenericType(protocol.getClass());
            // 获取配置类
            Config config = (Config) ApplicationUtils.getBean(configType);
            // 注册成员 for AP
            injectMembers4AP(config);
            // 协议初始化
            protocol.init(config);
            ProtocolManager.this.apProtocol = protocol;
        });
    }

    /**
     * 初始化 CP 协议
     */
    private void initCPProtocol() {
        ApplicationUtils.getBeanIfExist(CPProtocol.class, protocol -> {
            Class configType = ClassUtils.resolveGenericType(protocol.getClass());
            Config config = (Config) ApplicationUtils.getBean(configType);
            injectMembers4CP(config);
            protocol.init(config);
            ProtocolManager.this.cpProtocol = protocol;
        });
    }
    
    private void injectMembers4CP(Config config) {
        final Member selfMember = memberManager.getSelf();
        final String self = selfMember.getIp() + ":" + Integer
                .parseInt(String.valueOf(selfMember.getExtendVal(MemberMetaDataConstants.RAFT_PORT)));
        Set<String> others = toCPMembersInfo(memberManager.allMembers());
        config.setMembers(self, others);
    }
    
    private void injectMembers4AP(Config config) {
        final String self = memberManager.getSelf().getAddress();
        Set<String> others = toAPMembersInfo(memberManager.allMembers());
        config.setMembers(self, others);
    }
    
    @Override
    public void onEvent(MembersChangeEvent event) {
        // Here, the sequence of node change events is very important. For example,
        // node change event A occurs at time T1, and node change event B occurs at
        // time T2 after a period of time.
        // (T1 < T2)
        // Node change events between different protocols should not block each other.
        // and we use a single thread pool to inform the consistency layer of node changes,
        // to avoid multiple tasks simultaneously carrying out the consistency layer of
        // node changes operation
        if (Objects.nonNull(apProtocol)) {
            ProtocolExecutor.apMemberChange(() -> apProtocol.memberChange(toAPMembersInfo(event.getMembers())));
        }
        if (Objects.nonNull(cpProtocol)) {
            // CP的 raft端口
            ProtocolExecutor.cpMemberChange(() -> cpProtocol.memberChange(toCPMembersInfo(event.getMembers())));
        }
    }
}
