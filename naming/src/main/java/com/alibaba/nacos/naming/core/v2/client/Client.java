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

package com.alibaba.nacos.naming.core.v2.client;

import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.pojo.Subscriber;

import java.util.Collection;

/**
 * 代表一个客户端
 * Nacos naming client.
 *
 * <p>The abstract concept of the client stored by on the server of Nacos naming module. It is used to store which
 * services the client has published and subscribed.
 *
 * @author xiweng.yy
 */
public interface Client {
    
    /**
     * 从当前客户端获取一个唯一的id
     * Get the unique id of current client.
     *
     * @return id of client
     */
    String getClientId();
    
    /**
     * 当前客户端是否是临时实例
     * Whether is ephemeral of current client.
     *
     * @return true if client is ephemeral, otherwise false
     */
    boolean isEphemeral();
    
    /**
     * 更新 最后更新时间
     * Set the last time for updating current client as current time.
     */
    void setLastUpdatedTime();
    
    /**
     * 获取最后更新时间
     * Get the last time for updating current client.
     *
     * @return last time for updating
     */
    long getLastUpdatedTime();
    
    /**
     * 为当前客户端添加一个新的服务
     * Add a new instance for service for current client.
     *
     * @param service             publish service
     * @param instancePublishInfo instance
     * @return true if add successfully, otherwise false
     */
    boolean addServiceInstance(Service service, InstancePublishInfo instancePublishInfo);
    
    /**
     * 从当前客户端移除一个服务
     * Remove service instance from client.
     *
     * @param service service of instance
     * @return instance info if exist, otherwise {@code null}
     */
    InstancePublishInfo removeServiceInstance(Service service);
    
    /**
     * 从当前客户端中获取一个服务的实例信息
     * Get instance info of service from client.
     *
     * @param service service of instance
     * @return instance info
     */
    InstancePublishInfo getInstancePublishInfo(Service service);
    
    /**
     * 获取当前客户端的全部已发布服务
     * Get all published service of current client.
     *
     * @return published services
     */
    Collection<Service> getAllPublishedService();
    
    /**
     * 为一个目标服务添加一个新的订阅者
     * Add a new subscriber for target service.
     *
     * @param service    subscribe service
     * @param subscriber subscriber
     * @return true if add successfully, otherwise false
     */
    boolean addServiceSubscriber(Service service, Subscriber subscriber);
    
    /**
     * 将服务的订阅者移除
     * Remove subscriber for service.
     *
     * @param service service of subscriber
     * @return true if remove successfully, otherwise false
     */
    boolean removeServiceSubscriber(Service service);
    
    /**
     * 从服务中获取全部订阅者
     * Get subscriber of service from client.
     *
     * @param service service of subscriber
     * @return subscriber
     */
    Subscriber getSubscriber(Service service);
    
    /**
     * 获取当前客户端的全部订阅者
     * Get all subscribe service of current client.
     *
     * @return subscribe services
     */
    Collection<Service> getAllSubscribeService();
    
    /**
     * 生产同步数据
     * Generate sync data.
     *
     * @return sync data
     */
    ClientSyncData generateSyncData();
    
    /**
     * 判断当前客户端是否过期
     * Whether current client is expired.
     *
     * @param currentTime unified current timestamp
     * @return true if client has expired, otherwise false
     */
    boolean isExpire(long currentTime);
    
    /**
     * 释放当前客户端和必要的资源
     * Release current client and release resources if neccessary.
     */
    void release();
    
    /**
     * Recalculate client revision and get its value.
     * @return recalculated revision value
     */
    long recalculateRevision();
    
    /**
     * Get client revision.
     * @return current revision without recalculation
     */
    long getRevision();
    
    /**
     * Set client revision.
     * @param revision revision of this client to update
     */
    void setRevision(long revision);
    
}
