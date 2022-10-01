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

package com.alibaba.nacos.common.notify;

import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.alibaba.nacos.common.notify.NotifyCenter.ringBufferSize;

/**
 * 默认发布器
 * The default event publisher implementation.
 *
 * <p>Internally, use {@link ArrayBlockingQueue <Event/>} as a message staging queue.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 * @author zongtanghu
 */
public class DefaultPublisher extends Thread implements EventPublisher {
    
    protected static final Logger LOGGER = LoggerFactory.getLogger(NotifyCenter.class);

    /**
     * 初始化变量控制 只能启动一次线程
     */
    private volatile boolean initialized = false;
    
    private volatile boolean shutdown = false;
    
    private Class<? extends Event> eventType;

    /**
     * 存放 订阅者 TODO 内存中？集群中如何通讯 .
     */
    protected final ConcurrentHashSet<Subscriber> subscribers = new ConcurrentHashSet<>();
    
    private int queueMaxSize = -1;
    
    private BlockingQueue<Event> queue;
    
    protected volatile Long lastEventSequence = -1L;
    
    private static final AtomicReferenceFieldUpdater<DefaultPublisher, Long> UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(DefaultPublisher.class, Long.class, "lastEventSequence");
    
    @Override
    public void init(Class<? extends Event> type, int bufferSize) {
        setDaemon(true);
        setName("nacos.publisher-" + type.getName());
        this.eventType = type;
        this.queueMaxSize = bufferSize;
        this.queue = new ArrayBlockingQueue<>(bufferSize);
        start();
    }
    
    public ConcurrentHashSet<Subscriber> getSubscribers() {
        return subscribers;
    }
    
    @Override
    public synchronized void start() {
        if (!initialized) {
            // start just called once
            // 开启线程
            super.start();
            if (queueMaxSize == -1) {
                queueMaxSize = ringBufferSize;
            }
            initialized = true;
        }
    }
    
    @Override
    public long currentEventSize() {
        return queue.size();
    }
    
    @Override
    public void run() {
        // 线程运行、开启事件处理器
        openEventHandler();
    }
    
    void openEventHandler() {
        try {

            // 解决队列中 消息积压问题，怎么个解决法？
            // This variable is defined to resolve the problem which message overstock in the queue.
            int waitTimes = 60;
            // To ensure that messages are not lost, enable EventHandler when
            // waiting for the first Subscriber to register
            for (; ; ) {
                if (shutdown || hasSubscriber() || waitTimes <= 0) {
                    // 当关闭
                    // 有消费者了
                    // 等待事件够了
                    // 满足一个条件就退出循环
                    break;
                }
                ThreadUtils.sleep(1000L);
                // 代表这个循环最多循环60次、一次等待一秒、也就是最多等待1分钟
                waitTimes--;
            }
            
            for (; ; ) {
                if (shutdown) {
                    // 系统关闭就退出
                    break;
                }
                // 阻塞等待获取事件，从头部获取
                final Event event = queue.take();
                // 接收到一个事件 并且 通知消费者 处理事件
                receiveEvent(event);
                UPDATER.compareAndSet(this, lastEventSequence, Math.max(lastEventSequence, event.sequence()));
            }
        } catch (Throwable ex) {
            LOGGER.error("Event listener exception : ", ex);
        }
    }
    
    private boolean hasSubscriber() {
        return CollectionUtils.isNotEmpty(subscribers);
    }
    
    @Override
    public void addSubscriber(Subscriber subscriber) {
        subscribers.add(subscriber);
    }
    
    @Override
    public void removeSubscriber(Subscriber subscriber) {
        subscribers.remove(subscriber);
    }
    
    @Override
    public boolean publish(Event event) {
        checkIsStart();
        boolean success = this.queue.offer(event);
        if (!success) {
            LOGGER.warn("Unable to plug in due to interruption, synchronize sending time, event : {}", event);
            receiveEvent(event);
            return true;
        }
        return true;
    }
    
    void checkIsStart() {
        if (!initialized) {
            throw new IllegalStateException("Publisher does not start");
        }
    }
    
    @Override
    public void shutdown() {
        this.shutdown = true;
        this.queue.clear();
    }
    
    public boolean isInitialized() {
        return initialized;
    }
    
    /**
     * 接收到一个事件 并且 通知消费者 处理事件
     * Receive and notifySubscriber to process the event.
     *
     * @param event {@link Event}.
     */
    void receiveEvent(Event event) {
        // 事件序号
        final long currentEventSequence = event.sequence();
        
        if (!hasSubscriber()) {
            // 没消费者、直接退出、相当于事件直接就没了
            LOGGER.warn("[NotifyCenter] the {} is lost, because there is no subscriber.", event);
            return;
        }
        
        // Notification single event listener
        // 循环全部消费者
        for (Subscriber subscriber : subscribers) {
            if (!subscriber.scopeMatches(event)) {
                // 如果当前消费者不处理这个事件，就跳过本次
                continue;
            }
            
            // Whether to ignore expiration events
            if (subscriber.ignoreExpireEvent() && lastEventSequence > currentEventSequence) {
                // 事件已过期，跳过本次
                LOGGER.debug("[NotifyCenter] the {} is unacceptable to this subscriber, because had expire",
                        event.getClass());
                continue;
            }

            // 通知消费者
            // Because unifying smartSubscriber and subscriber, so here need to think of compatibility.
            // Remove original judge part of codes.
            notifySubscriber(subscriber, event);
        }
    }

    /**
     * 通知消费者
     * @param subscriber {@link Subscriber}
     * @param event      {@link Event}
     */
    @Override
    public void notifySubscriber(final Subscriber subscriber, final Event event) {
        
        LOGGER.debug("[NotifyCenter] the {} will received by {}", event, subscriber);
        
        final Runnable job = () -> subscriber.onEvent(event);
        final Executor executor = subscriber.executor();

        // 如果这个消费者本身有线程池就用线程池，否则就同步调用
        if (executor != null) {
            executor.execute(job);
        } else {
            try {
                // 注意，不使用线程， 同步执行
                job.run();
            } catch (Throwable e) {
                LOGGER.error("Event callback exception: ", e);
            }
        }
    }
}
