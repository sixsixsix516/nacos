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

package com.alibaba.nacos.core.cluster;

/**
 * The life cycle state of a node plays an important role.
 *
 * <p>1.3.0 The unified sinking operation should be done first, and the node state
 * should be radiated out later, mainly for whether the request can be processed and so on</p>
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public enum NodeState {
    
    /**
     * 启动中
     * Node is starting.
     */
    STARTING,
    
    /**
     * 启动，接收请求
     * Node is up and ready for request.
     */
    UP,
    
    /**
     * 节点疑似崩溃
     * Node may Crash.
     */
    SUSPICIOUS,
    
    /**
     * 节点下线
     * Node is out of service, something abnormal happened.
     */
    DOWN,
    
    /**
     * 节点隔离
     * The Node is isolated.
     */
    ISOLATION,
    
}
