/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibabacloud.polar_race.engine.common.common;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class BasicThreadFactory implements ThreadFactory {
    private final AtomicInteger threadCounter = new AtomicInteger();

    private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

    private final String namingPattern;

    private final Integer priority;

    private final Boolean daemon;

    private Class<? extends Thread> clazz;

    private Constructor<? extends Thread> constructor;

    private BasicThreadFactory(final Builder builder) {
        namingPattern = builder.namingPattern;
        priority = builder.priority;
        daemon = builder.daemon;
        uncaughtExceptionHandler = builder.exceptionHandler;
        if (builder.clazz != null) {
            this.clazz = builder.clazz;
            try {
                constructor = clazz.getConstructor(Runnable.class);
                constructor.setAccessible(true);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public final String getNamingPattern() {
        return namingPattern;
    }

    public final Boolean getDaemonFlag() {
        return daemon;
    }


    public final Integer getPriority() {
        return priority;
    }

    public final Thread.UncaughtExceptionHandler getUncaughtExceptionHandler() {
        return uncaughtExceptionHandler;
    }

    public long getThreadCount() {
        return threadCounter.get();
    }

    @Override
    public Thread newThread(final Runnable runnable) {
        Thread thread;
        if (clazz != null) {
            try {
                thread = constructor.newInstance(runnable);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        } else {
            thread = new Thread(runnable);
        }
        initializeThread(thread);
        return thread;
    }


    private void initializeThread(final Thread thread) {

        if (getNamingPattern() != null) {
            final int count = threadCounter.incrementAndGet();
            thread.setName(String.format(getNamingPattern(), count));
        }

        if (getUncaughtExceptionHandler() != null) {
            thread.setUncaughtExceptionHandler(getUncaughtExceptionHandler());
        }

        if (getPriority() != null) {
            thread.setPriority(getPriority());
        }

        if (getDaemonFlag() != null) {
            thread.setDaemon(getDaemonFlag());
        }
    }

    public static class Builder {

        private Thread.UncaughtExceptionHandler exceptionHandler;

        private String namingPattern;

        private Integer priority;

        private Boolean daemon;

        private Class<? extends Thread> clazz;


        public Builder namingPattern(final String pattern) {
            Objects.requireNonNull(pattern, "Naming pattern must not be null!");

            namingPattern = pattern;
            return this;
        }

        public Builder daemon(final boolean daemon) {
            this.daemon = daemon;
            return this;
        }


        public Builder priority(final int priority) {
            this.priority = priority;
            return this;
        }

        public Builder uncaughtExceptionHandler(
                final Thread.UncaughtExceptionHandler handler) {
            Objects.requireNonNull(handler, "Uncaught exception handler must not be null!");
            exceptionHandler = handler;
            return this;
        }

        public Builder clazz(Class<? extends Thread> clazz) {
            Objects.requireNonNull(clazz);
            this.clazz = clazz;
            return this;
        }

        public void reset() {
            exceptionHandler = null;
            namingPattern = null;
            priority = null;
            daemon = null;
        }

        public BasicThreadFactory build() {
            final BasicThreadFactory factory = new BasicThreadFactory(this);
            reset();
            return factory;
        }
    }
}
