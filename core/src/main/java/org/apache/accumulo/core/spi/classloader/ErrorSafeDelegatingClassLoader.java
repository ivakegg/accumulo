/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.spi.classloader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.stream.Stream;

public class ErrorSafeDelegatingClassLoader extends ClassLoader {

    private static final Logger log = LoggerFactory.getLogger(ErrorSafeDelegatingClassLoader.class);
    private final ClassLoader delegate;

    public ErrorSafeDelegatingClassLoader(ClassLoader delegate) {
        super("ErrorSafeDelegatingClassLoader(" + delegate.getName() + ")", delegate);
        this.delegate = delegate;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        try {
            return delegate.loadClass(name);
        } catch (LinkageError e) {
            log.warn("Failed to load class " + name, e);
            throw new ClassNotFoundException(e.getMessage());
        }
    }

    @Override
    public URL getResource(String name) {
        return delegate.getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        return delegate.getResources(name);
    }

    @Override
    public Stream<URL> resources(String name) {
        return delegate.resources(name);
    }

    @Override
    public InputStream getResourceAsStream(String name) {
        return delegate.getResourceAsStream(name);
    }

    @Override
    public void setDefaultAssertionStatus(boolean enabled) {
        delegate.setDefaultAssertionStatus(enabled);
    }

    @Override
    public void setPackageAssertionStatus(String packageName, boolean enabled) {
        delegate.setPackageAssertionStatus(packageName, enabled);
    }

    @Override
    public void setClassAssertionStatus(String className, boolean enabled) {
        delegate.setClassAssertionStatus(className, enabled);
    }

    @Override
    public void clearAssertionStatus() {
        delegate.clearAssertionStatus();
    }
}
