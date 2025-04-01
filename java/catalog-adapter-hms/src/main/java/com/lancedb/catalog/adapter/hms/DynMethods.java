/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lancedb.catalog.adapter.hms;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;

/** Copied from parquet-common */
public class DynMethods {

  private DynMethods() {}

  /**
   * Convenience wrapper class around {@link Method}.
   *
   * <p>Allows callers to invoke the wrapped method with all Exceptions wrapped by RuntimeException,
   * or with a single Exception catch block.
   */
  public static class UnboundMethod {

    private final Method method;
    private final String name;
    private final int argLength;

    UnboundMethod(Method method, String name) {
      this.method = method;
      this.name = name;
      this.argLength =
          (method == null || method.isVarArgs()) ? -1 : method.getParameterTypes().length;
    }

    @SuppressWarnings("unchecked")
    <R> R invokeChecked(Object target, Object... args) throws Exception {
      try {
        if (argLength < 0) {
          return (R) method.invoke(target, args);
        } else {
          return (R) method.invoke(target, Arrays.copyOfRange(args, 0, argLength));
        }

      } catch (InvocationTargetException e) {
        Throwables.propagateIfInstanceOf(e.getCause(), Exception.class);
        Throwables.propagateIfInstanceOf(e.getCause(), RuntimeException.class);
        throw Throwables.propagate(e.getCause());
      }
    }

    public <R> R invoke(Object target, Object... args) {
      try {
        return this.invokeChecked(target, args);
      } catch (Exception e) {
        Throwables.propagateIfInstanceOf(e, RuntimeException.class);
        throw Throwables.propagate(e);
      }
    }

    /**
     * Returns this method as a BoundMethod for the given receiver.
     *
     * @param receiver an Object to receive the method invocation
     * @return a {@link BoundMethod} for this method and the receiver
     * @throws IllegalStateException if the method is static
     * @throws IllegalArgumentException if the receiver's class is incompatible
     */
    public BoundMethod bind(Object receiver) {
      Preconditions.checkState(
          !isStatic(), "Cannot bind static method %s", method.toGenericString());
      Preconditions.checkArgument(
          method.getDeclaringClass().isAssignableFrom(receiver.getClass()),
          "Cannot bind %s to instance of %s",
          method.toGenericString(),
          receiver.getClass());

      return new BoundMethod(this, receiver);
    }

    /** Returns whether the method is a static method. */
    public boolean isStatic() {
      return Modifier.isStatic(method.getModifiers());
    }

    /** Returns whether the method is a noop. */
    public boolean isNoop() {
      return this == NOOP;
    }

    /**
     * Returns this method as a StaticMethod.
     *
     * @return a {@link StaticMethod} for this method
     * @throws IllegalStateException if the method is not static
     */
    public StaticMethod asStatic() {
      Preconditions.checkState(isStatic(), "Method is not static");
      return new StaticMethod(this);
    }

    @Override
    public String toString() {
      return "DynMethods.UnboundMethod(name=" + name + " method=" + method.toGenericString() + ")";
    }

    /** Singleton {@link UnboundMethod}, performs no operation and returns null. */
    private static final UnboundMethod NOOP =
        new UnboundMethod(null, "NOOP") {
          /** @deprecated since 1.7.0, visibility will be reduced in 1.8.0 */
          @Deprecated // will become package-private
          @Override
          public <R> R invokeChecked(Object target, Object... args) {
            return null;
          }

          @Override
          public BoundMethod bind(Object receiver) {
            return new BoundMethod(this, receiver);
          }

          @Override
          public StaticMethod asStatic() {
            return new StaticMethod(this);
          }

          @Override
          public boolean isStatic() {
            return true;
          }

          @Override
          public String toString() {
            return "DynMethods.UnboundMethod(NOOP)";
          }
        };
  }

  public static class BoundMethod {
    private final UnboundMethod method;
    private final Object receiver;

    private BoundMethod(UnboundMethod method, Object receiver) {
      this.method = method;
      this.receiver = receiver;
    }

    public <R> R invokeChecked(Object... args) throws Exception {
      return method.invokeChecked(receiver, args);
    }

    public <R> R invoke(Object... args) {
      return method.invoke(receiver, args);
    }
  }

  public static class StaticMethod {
    private final UnboundMethod method;

    private StaticMethod(UnboundMethod method) {
      this.method = method;
    }

    public <R> R invokeChecked(Object... args) throws Exception {
      return method.invokeChecked(null, args);
    }

    public <R> R invoke(Object... args) {
      return method.invoke(null, args);
    }
  }

  /**
   * Constructs a new builder for calling methods dynamically.
   *
   * @param methodName name of the method the builder will locate
   * @return a Builder for finding a method
   */
  public static Builder builder(String methodName) {
    return new Builder(methodName);
  }

  public static class Builder {
    private final String name;
    private ClassLoader loader = Thread.currentThread().getContextClassLoader();
    private UnboundMethod method = null;

    public Builder(String methodName) {
      this.name = methodName;
    }

    /**
     * Set the {@link ClassLoader} used to lookup classes by name.
     *
     * <p>If not set, the current thread's ClassLoader is used.
     *
     * @param newLoader a ClassLoader
     * @return this Builder for method chaining
     */
    public Builder loader(ClassLoader newLoader) {
      this.loader = newLoader;
      return this;
    }

    /**
     * If no implementation has been found, adds a NOOP method.
     *
     * <p>Note: calls to impl will not match after this method is called!
     *
     * @return this Builder for method chaining
     */
    public Builder orNoop() {
      if (method == null) {
        this.method = UnboundMethod.NOOP;
      }
      return this;
    }

    /**
     * Checks for an implementation, first finding the given class by name.
     *
     * @param className name of a class
     * @param methodName name of a method (different from constructor)
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getMethod(String, Class[])
     */
    public Builder impl(String className, String methodName, Class<?>... argClasses) {
      // don't do any work if an implementation has been found
      if (method != null) {
        return this;
      }

      try {
        Class<?> targetClass = Class.forName(className, true, loader);
        impl(targetClass, methodName, argClasses);
      } catch (ClassNotFoundException e) {
        // not the right implementation
      }
      return this;
    }

    /**
     * Checks for an implementation, first finding the given class by name.
     *
     * <p>The name passed to the constructor is the method name used.
     *
     * @param className name of a class
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getMethod(String, Class[])
     */
    public Builder impl(String className, Class<?>... argClasses) {
      impl(className, name, argClasses);
      return this;
    }

    /**
     * Checks for a method implementation.
     *
     * @param targetClass a class instance
     * @param methodName name of a method (different from constructor)
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getMethod(String, Class[])
     */
    public Builder impl(Class<?> targetClass, String methodName, Class<?>... argClasses) {
      // don't do any work if an implementation has been found
      if (method != null) {
        return this;
      }

      try {
        this.method = new UnboundMethod(targetClass.getMethod(methodName, argClasses), name);
      } catch (NoSuchMethodException e) {
        // not the right implementation
      }
      return this;
    }

    /**
     * Checks for a method implementation.
     *
     * <p>The name passed to the constructor is the method name used.
     *
     * @param targetClass a class instance
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getMethod(String, Class[])
     */
    public Builder impl(Class<?> targetClass, Class<?>... argClasses) {
      impl(targetClass, name, argClasses);
      return this;
    }

    /**
     * Checks for an implementation, first finding the given class by name.
     *
     * @param className name of a class
     * @param methodName name of a method (different from constructor)
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getMethod(String, Class[])
     */
    public Builder hiddenImpl(String className, String methodName, Class<?>... argClasses) {
      // don't do any work if an implementation has been found
      if (method != null) {
        return this;
      }

      try {
        Class<?> targetClass = Class.forName(className, true, loader);
        hiddenImpl(targetClass, methodName, argClasses);
      } catch (ClassNotFoundException e) {
        // not the right implementation
      }
      return this;
    }

    /**
     * Checks for an implementation, first finding the given class by name.
     *
     * <p>The name passed to the constructor is the method name used.
     *
     * @param className name of a class
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getMethod(String, Class[])
     */
    public Builder hiddenImpl(String className, Class<?>... argClasses) {
      hiddenImpl(className, name, argClasses);
      return this;
    }

    /**
     * Checks for a method implementation.
     *
     * @param targetClass a class instance
     * @param methodName name of a method (different from constructor)
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getMethod(String, Class[])
     */
    public Builder hiddenImpl(Class<?> targetClass, String methodName, Class<?>... argClasses) {
      // don't do any work if an implementation has been found
      if (method != null) {
        return this;
      }

      try {
        Method hidden = targetClass.getDeclaredMethod(methodName, argClasses);
        AccessController.doPrivileged(new MakeAccessible(hidden));
        this.method = new UnboundMethod(hidden, name);
      } catch (SecurityException | NoSuchMethodException e) {
        // unusable or not the right implementation
      }
      return this;
    }

    /**
     * Checks for a method implementation.
     *
     * <p>The name passed to the constructor is the method name used.
     *
     * @param targetClass a class instance
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getMethod(String, Class[])
     */
    public Builder hiddenImpl(Class<?> targetClass, Class<?>... argClasses) {
      hiddenImpl(targetClass, name, argClasses);
      return this;
    }

    /**
     * Returns the first valid implementation as a UnboundMethod or throws a RuntimeError if there
     * is none.
     *
     * @return a {@link UnboundMethod} with a valid implementation
     * @throws RuntimeException if no implementation was found
     */
    public UnboundMethod build() {
      if (method != null) {
        return method;
      } else {
        throw new RuntimeException("Cannot find method: " + name);
      }
    }

    /**
     * Returns the first valid implementation as a BoundMethod or throws a RuntimeError if there is
     * none.
     *
     * @param receiver an Object to receive the method invocation
     * @return a {@link BoundMethod} with a valid implementation and receiver
     * @throws IllegalStateException if the method is static
     * @throws IllegalArgumentException if the receiver's class is incompatible
     * @throws RuntimeException if no implementation was found
     */
    public BoundMethod build(Object receiver) {
      return build().bind(receiver);
    }

    /**
     * Returns the first valid implementation as a UnboundMethod or throws a NoSuchMethodException
     * if there is none.
     *
     * @return a {@link UnboundMethod} with a valid implementation
     * @throws NoSuchMethodException if no implementation was found
     */
    public UnboundMethod buildChecked() throws NoSuchMethodException {
      if (method != null) {
        return method;
      } else {
        throw new NoSuchMethodException("Cannot find method: " + name);
      }
    }

    /**
     * Returns the first valid implementation as a BoundMethod or throws a NoSuchMethodException if
     * there is none.
     *
     * @param receiver an Object to receive the method invocation
     * @return a {@link BoundMethod} with a valid implementation and receiver
     * @throws IllegalStateException if the method is static
     * @throws IllegalArgumentException if the receiver's class is incompatible
     * @throws NoSuchMethodException if no implementation was found
     */
    public BoundMethod buildChecked(Object receiver) throws NoSuchMethodException {
      return buildChecked().bind(receiver);
    }

    /**
     * Returns the first valid implementation as a StaticMethod or throws a NoSuchMethodException if
     * there is none.
     *
     * @return a {@link StaticMethod} with a valid implementation
     * @throws IllegalStateException if the method is not static
     * @throws NoSuchMethodException if no implementation was found
     */
    public StaticMethod buildStaticChecked() throws NoSuchMethodException {
      return buildChecked().asStatic();
    }

    /**
     * Returns the first valid implementation as a StaticMethod or throws a RuntimeException if
     * there is none.
     *
     * @return a {@link StaticMethod} with a valid implementation
     * @throws IllegalStateException if the method is not static
     * @throws RuntimeException if no implementation was found
     */
    public StaticMethod buildStatic() {
      return build().asStatic();
    }
  }

  private static class MakeAccessible implements PrivilegedAction<Void> {
    private final Method hidden;

    MakeAccessible(Method hidden) {
      this.hidden = hidden;
    }

    @Override
    public Void run() {
      hidden.setAccessible(true);
      return null;
    }
  }
}
