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

package com.alibaba.nacos.naming.pojo;

import java.io.Serializable;

/**
 * 要在Nacos集群中传输和存储的记录
 * <p>
 * Record to transfer and store in Nacos cluster.
 *
 * @author nkorange
 * @since 1.0.0
 */
public interface Record extends Serializable {
    
    /**
     * 获取这条记录的校验和，通常用户记录比较
     * get the checksum of this record, usually for record comparison.
     *
     * @return checksum of record
     */
    String getChecksum();
}
