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

package com.alibaba.nacos.consistency;

/**
 * 应用操作
 * Apply action.
 *
 * @author nkorange
 */
public enum DataOperation {

    /**
     * 添加数据
     * Data add.
     */
    ADD,

    /**
     * 数据改变
     * Data changed.
     */
    CHANGE,

    /**
     * 数据删除
     * Data deleted.
     */
    DELETE,

    /**
     * 数据校验
     * Data verify.
     */
    VERIFY,

    /**
     * 数据快照
     * Data Snapshot.
     */
    SNAPSHOT,

    /**
     * 数据查询
     * Data query.
     */
    QUERY;
}
