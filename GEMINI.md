# Using Gemini Code Assist in this Project

You are an expert assistant for developers working on GraalVM Native Image support for Google Cloud Java client libraries. Your goal is to help diagnose build failures, answer questions about configuration, and provide guidance based on these team's best practices.

## Core Concepts
**What is Native Image?** It's a technology that compiles Java code ahead-of-time into a standalone executable. This results in significant performance benefits, such as fast startup times (e.g., 25ms for a client library vs. 600-1000ms on a standard JVM) and lower memory usage because it doesn't require a JVM.
**The Challenge:** Native image compilation can be incompatible with dynamic Java features like reflection or resource loading. Therefore, explicit configuration is often required to make libraries work correctly ( source).

## Understanding Tests in Native Image Compilation

We use the
[native-maven-plugin](https://graalvm.github.io/native-build-tools/0.9.16/maven-plugin.html#introduction)
to run tests. The standard tests run first and then the native image compilation
runs. Note that if you see tests failing before the native compilation phase
(shown below), it is likely that the test is failing with standard Java. The
order is as follows:

1) Run tests in standard Java.
[Native tests depend on the standard tests](https://medium.com/graalvm/gradle-and-maven-plugins-for-native-image-with-initial-junit-testing-support-dde00a8caf0b)
to run beforehand.

2) Start Native Image build to generate a native image for the tests being run.

The start of native image testing is indicated by this line in the logs:

```
[INFO] --- native-maven-plugin:0.9.13:test (test-native) @ google-cloud-bigtable-stats ---
[INFO] ====================
[INFO] Initializing project: google-cloud-bigtable-stats
[INFO] ====================

```

Native Image Build phase:

```
====================================================================================================
GraalVM Native Image: Generating 'native-tests' (executable)...
====================================================================================================
[1/7] Initializing...                                                               (14.5s @ 0.71GB)
 Version info: 'GraalVM 22.2.0 Java 11 CE'
 Java version info: '11.0.16+8-jvmci-22.2-b06'
 C compiler: gcc (linux, x86_64, 12.2.0)
 Garbage collector: Serial GC
 6 user-specific feature(s)
 - com.google.api.gax.grpc.nativeimage.GrpcNettyFeature
 - com.google.api.gax.grpc.nativeimage.ProtobufMessageFeature
 - com.google.api.gax.nativeimage.GoogleJsonClientFeature
 - com.google.api.gax.nativeimage.OpenCensusFeature
 - com.oracle.svm.thirdparty.gson.GsonFeature
 - org.graalvm.junit.platform.JUnitPlatformFeature
[junit-platform-native] Running in 'test listener' mode using files matching pattern [junit-platform-unique-ids*] found in folder [/usr/local/google/home/mpeddada/IdeaProjects/java-bigtable/google-cloud-bigtable-stats/target/test-ids] and its subfolders.

[2/7] Performing analysis...  [***********]                                         (20.3s @ 3.47GB)
   8,030 (85.79%) of  9,360 classes reachable
  11,613 (58.29%) of 19,924 fields reachable
  41,126 (57.31%) of 71,761 methods reachable
     446 classes,   942 fields, and 6,664 methods registered for reflection
      68 classes,    87 fields, and    56 methods registered for JNI access
       5 native libraries: dl, pthread, rt, stdc++, z
[3/7] Building universe...                                                           (3.9s @ 1.12GB)
[4/7] Parsing methods...      [*]                                                    (1.6s @ 3.50GB)
[5/7] Inlining methods...     [***]                                                  (1.0s @ 4.69GB)
[6/7] Compiling methods...    [***]                                                 (11.3s @ 3.46GB)
[7/7] Creating image...                                                              (2.9s @ 4.53GB)
  15.18MB (40.88%) for code area:    27,662 compilation units
  19.96MB (53.77%) for image heap:  207,237 objects and 15 resources
   1.98MB ( 5.34%) for other data
  37.13MB in total

```

3) Tests Run as Native Images. This step should be considerably faster than
running standard Java tests.

```
com.google.cloud.spanner.connection.it.ITBulkConnectionTest > testBulkCreateConnectionsMultiThreaded SUCCESSFUL

com.google.cloud.spanner.connection.it.ITBulkConnectionTest > testBulkCreateConnectionsSingleThreaded SUCCESSFUL

```

## Troubleshooting Common Issues

This section describes ways to spot common Native Image compilation issues.

### Mocks

Mockito dynamically loads classes at run-time which doesn’t work too well with
native image compilation at the moment. You may see errors like this when you
try to compile a test using mocks with native image compilation:

```
java.lang.NoClassDefFoundError: Could not initialize class org.mockito.internal.configuration.plugins.Plugins
       org.mockito.internal.configuration.GlobalConfiguration.tryGetPluginAnnotationEngine(GlobalConfiguration.java:50)
       org.mockito.MockitoAnnotations.openMocks(MockitoAnnotations.java:81)
       org.mockito.MockitoAnnotations.initMocks(MockitoAnnotations.java:100)
       com.google.cloud.spanner.SessionClientTest.setUp(SessionClientTest.java:90)
       java.lang.reflect.Method.invoke(Method.java:566)
       org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:59)
       org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
       org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:56)
```

As a result, we try to skip unit tests in handwritten libraries and run only
**integration tests in all libraries** and **unit tests in generated
libraries**. To that end, it is recommended that we don’t use mocking libraries
such as Mockito or EasyMock in integration tests.

### Testing Frameworks

#### Truth vs Hamcrest: Use Truth

Testing with Hamcrest can sometimes cause problems, as seen below:

```
java.lang.Error: Cannot determine correct type for matchesSafely() method.
       org.hamcrest.internal.ReflectiveTypeFinder.findExpectedType(ReflectiveTypeFinder.java:49)
       org.hamcrest.TypeSafeMatcher.<init>(TypeSafeMatcher.java:40)
       org.hamcrest.TypeSafeMatcher.<init>(TypeSafeMatcher.java:22)
       org.hamcrest.core.SubstringMatcher.<init>(SubstringMatcher.java:13)
       org.hamcrest.core.StringStartsWith.<init>(StringStartsWith.java:13)
       org.hamcrest.core.StringStartsWith.startsWith(StringStartsWith.java:38)
       org.hamcrest.CoreMatchers.startsWith(CoreMatchers.java:516)

```

In this case, our test was using `assertThat(statement, actual, startsWith())`
which included some reflection usages that were incompatible with native image
compilation.

In such a scenario, it is possible to add more reflection configuration (at the
cost of the size of the native image). However, whenever possible, try using the
Truth framework. Truth provides clear error messages and is also more readable.

#### Leniency in Assertions

Another Hamcrest-related issue can manifest as `Error: Cannot determine correct
type for matchesSafely() method` (example:
[java-iam-admin#58](https://github.com/googleapis/java-iam-admin/pull/58)). In
this case, we were using `assumeThat` which made use of some reflection usages.
This resulted in the following error message:

```
java.lang.Error: Cannot determine correct type for matchesSafely() method.
      org.hamcrest.internal.ReflectiveTypeFinder.findExpectedType(ReflectiveTypeFinder.java:49)
       org.hamcrest.TypeSafeDiagnosingMatcher.<init>(TypeSafeDiagnosingMatcher.java:42)
       org.hamcrest.TypeSafeDiagnosingMatcher.<init>(TypeSafeDiagnosingMatcher.java:49)
       org.hamcrest.core.Every.<init>(Every.java:11)
       org.hamcrest.core.Every.everyItem(Every.java:45)
       org.hamcrest.CoreMatchers.everyItem(CoreMatchers.java:196)
       org.junit.Assume.assumeNotNull(Assume.java:84)
       com.google.cloud.iam.admin.v1.it.ITSystemTest.listServiceAccounts(ITSystemTest.java:47)
       java.lang.reflect.Method.invoke(Method.java:566)
       org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:59)

```

The `assumeThat` in this particular situation was being used to enable the
integration test to run. The native image compilation issue was addressed by
using a stricter check (`assertThat`) which removed the need for extra
configurations and also allowed for consistency in the integration tests being
run through profiles (`-P enable-integration-tests`).

### Test Resources

Sometimes test resources cannot be recognized by the native image compiler and
you may run into following error :

```
 JUnit Vintage:ITBigQueryTest
    ClassSource [className = 'com.google.cloud.bigquery.it.ITBigQueryTest', filePosition = null]
    => java.nio.file.NoSuchFileException: src/test/resources/QueryTestData.csv
       sun.nio.fs.UnixFileSystemProvider.newByteChannel(UnixFileSystemProvider.java:219)
       java.nio.file.Files.newByteChannel(Files.java:371)
       java.nio.file.Files.newByteChannel(Files.java:422)
       java.nio.file.spi.FileSystemProvider.newInputStream(FileSystemProvider.java:420)
       java.nio.file.Files.newInputStream(Files.java:156)
       com.google.cloud.storage.StorageImpl.createFrom(StorageImpl.java:215)
       com.google.cloud.storage.StorageImpl.createFrom(StorageImpl.java:206)
       com.google.cloud.bigquery.it.ITBigQueryTest.beforeClass(ITBigQueryTest.java:473)
       java.lang.reflect.Method.invoke(Method.java:566)
       org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:59)
       [...]
```

Resources need to be registered in order to be accessible at run-time. You can
do this by creating a resource-config.json or using the Feature interface. Here
is an example:
[java-bigquery/pull/1859/](https://github.com/googleapis/java-bigquery/pull/1859/files#diff-99abac001c501cf9db9fb18cae982aeee9a4c9ace161636b590fdbe25df7cb02)

**Best Practice for referencing Resource**: These resource-based configurations
are recognized by
[`Class.getResource()` or `Class.getResourceAsStream()`](https://github.com/oracle/graal/blob/vm-22.2.0/docs/reference-manual/native-image/Resources.md)
or other `ClassLoader` methods. Whenever possible, try using these methods to
reference resources in tests. It is always recommended to resolve resources as
classpath resources as opposed to files. An example PR can be found
[here](https://github.com/googleapis/java-bigquery/pull/1859).

### Test Config File Not Found

You may run into an issue where the native image compilation fails with the
following error message:

```
Test configuration file wasn't found. Make sure that test execution wasn't skipped.
```

This means the native image test plugin is looking for tests that match
"IT*.java" or "*ClientTest.java" pattern defined in
[the "native" profile](https://github.com/googleapis/java-shared-config/blob/main/pom.xml).
But the project does not contain the matching Java files.

This is a known
[limitation](https://github.com/graalvm/native-build-tools/issues/188) of the
native-maven-plugin and occurs when there are no native image tests to run in a
module.

If the Maven project cannot run native image tests, then declare
`<skipNativeTests>true</skipNativeTests>`
([document](https://graalvm.github.io/native-build-tools/latest/maven-plugin.html#configuration-options))
in the properties section of the pom.xml file (example:
https://github.com/googleapis/google-cloud-java/pull/9076/files).

### A required class was missing ... org/graalvm/nativeimage/hosted/Feature

The following error is a typical error when you're not using the Java compiler
of a GraalVM distribution.

```
Error:  Failed to execute goal org.graalvm.buildtools:native-maven-plugin:0.9.16:test
(test-native) on project google-cloud-orgpolicy: Execution test-native of goal
org.graalvm.buildtools:native-maven-plugin:0.9.16:test failed: A required class
was missing while executing org.graalvm.buildtools:native-maven-plugin:0.9.16:test:
org/graalvm/nativeimage/hosted/Feature
```

Confirm your `java -version` shows "GraalVM":

```
$ java -version
openjdk 11.0.16 2022-07-19
OpenJDK Runtime Environment GraalVM CE 22.2.0 (build 11.0.16+8-jvmci-22.2-b06)
OpenJDK 64-Bit Server VM GraalVM CE 22.2.0 (build 11.0.16+8-jvmci-22.2-b06, mixed mode, sharing)
```

If it's not showing "GraalVM", then read "Setup Development Environment" below.

### NoSuchMethodException on GeneratedMessage

For efficient image build, GraalVM native image compilation removes unnecessary
classes and methods. While it tries to auto-detect some of the reflections
([document](https://docs.oracle.com/en/graalvm/enterprise/20/docs/reference-manual/native-image/Reflection/)),
it does not take non-obvious reflection into account. Due to this limitation, a
reflection on a method can fail with `NoSuchMethodException`. An example error
message:

```
Caused by: java.lang.RuntimeException: Generated message class "com.google.api.FieldBehavior" missing method "valueOf".
  com.google.protobuf.GeneratedMessage.getMethodOrDie(GeneratedMessage.java:1973)
  com.google.protobuf.GeneratedMessage.access$1100(GeneratedMessage.java:61)
  com.google.protobuf.GeneratedMessage$GeneratedExtension.<init>(GeneratedMessage.java:1789)
  com.google.protobuf.GeneratedMessage.newFileScopedGeneratedExtension(GeneratedMessage.java:1643)
  com.google.api.FieldBehaviorProto.<clinit>(FieldBehaviorProto.java:55)
  [...]
Caused by: java.lang.NoSuchMethodException: com.google.api.FieldBehavior.valueOf(com.google.protobuf.Descriptors$EnumValueDescriptor)
  java.lang.Class.getMethod(DynamicHub.java:1114)
  com.google.protobuf.GeneratedMessage.getMethodOrDie(GeneratedMessage.java:1970)
  [...]
```

In this case, declare the dependency to `com.google.api:gax-grpc`, which has
`com.google.api.gax.grpc.nativeimage.ProtobufMessageFeature` to configure the
Protobuf-generated classes for native image compilation (Example:
[java-compute#688](https://github.com/googleapis/java-compute/pull/688/files).

### class file has wrong version 55.0, should be 53.0

Cloud Java libraries target Java 8 but GraalVM 22.1.0 started compiling their
source code in JDK 11 and JDK 8 cannot comipile Java code that imports their
classes. When the error occurs, you see an error like this:

```
T:\src\github\cloud-sql-jdbc-socket-factory\core\src\main\java\com\google\cloud\sql\nativeimage\CloudSqlFeature.java:[22,30] error: cannot access ImageSingletons

  bad class file: C:\Users\kbuilder\.m2\repository\org\graalvm\sdk\graal-sdk\22.1.0\graal-sdk-22.1.0.jar(/org/graalvm/nativeimage/ImageSingletons.class)

    class file has wrong version 55.0, should be 53.0
```

In the message, `CloudSqlFeature.java` is our source code that imports GraalVM's
class `ImageSingletons`. Java class file version 55.0 corresponds to Java 11 and
version 53.0 corresponds to Java 8.

A solution is compiling the code in JDK 11. To ensure the compiled bytecode
works in Java 8, we test the compiled classes in JDK 8 (example:
[gax-java#1671](https://github.com/googleapis/gax-java/pull/1671).

### Property 'Args' contains invalid entry '--add-modules=jdk.httpserver'

When running `native-image` command, you may get the following error message:

```
[INFO] Executing: /Users/suztomo/.sdkman/candidates/java/21.3.0.r11-grl/bin/native-image -cp /Users/suztomo/google-cloud-java/java-core/google-cloud-core/target/classes:/Users/suztomo/...
Caused by: com.oracle.svm.driver.NativeImage$NativeImageError: Property 'Args' contains invalid entry '--add-modules=jdk.httpserver'
```

This error message happens when you use an outdated `native-image` command.
Check the `GRAALVM_HOME` environment variable, `which native-image` command, and
`native-image --version` command to ensure you're using the correct version.

### NoClassDefFoundError: Could not initialize class

Example error:

```
java.lang.NoClassDefFoundError: Could not initialize class com.google.cloud.spanner.connection.AbstractStatementParser
com.google.cloud.spanner.connection.ConnectionImpl.getStatementParser(ConnectionImpl.java:320)
com.google.cloud.spanner.connection.ConnectionImpl.parseAndExecuteQuery(ConnectionImpl.java:1145)
com.google.cloud.spanner.connection.ConnectionImpl.executeQuery(ConnectionImpl.java:1016)
com.google.cloud.spanner.connection.it.ITBulkConnectionTest.testBulkCreateConnectionsSingleThreaded(ITBulkConnectionTest.java:54)
```

This error indicates an exception occurred during static initialization. It is
not the same error as `ClassNotFoundException` which means the class is not
available on the classpath (or has not been included from the native image due
to dynamic reflection logic).

If you encounter this error, refactor the class (in this example:
`AbstractStatementParser`) to place all complex static initialization logic in a
static try-catch block.

Example:

```java
class MyClass {
    // Bad! A thrown exception here will produce a cryptic NoClassDefFoundError
    private static final COMPLEX_SETUP = setupComplexThing();
}
```

```java
class MyClass {
    // Good! Exposes the root cause of the failure
    private static final COMPLEX_SETUP;
    static {
        try {
            COMPLEX_SETUP = setupComplexThing();
        } catch (Throwable ex) {
            // MyClass.class reference is safe, even if setupComplexThing fails.
            Logger.getLogger(MyClass.class.getName()).log(/* ... */, ex);
            throw ex;
        }
    }
}
```

### Error: Classes that should be initialized at run time got initialized during image building

A class may be initialized at native image build time when the configuration
file specifies the argument to do so. The classes referenced by such classes are
also initialized at build time recursively. The *"Class that should be
initialized at run time ..."* error indicates that the class in the error
message was (transitively or directly) referenced by a class that is marked as
build-time initialization.

To diagnose the error, pass `--trace-class-initialization=<fully qualified class
name>` argument to the `native-image` command printed in the error message. For
a native image test failure in Maven projects, you can modify the plugin
configuration in pom.xml (example:
[java-storage-nio troubleshooting in June 2024](https://github.com/googleapis/java-storage-nio/pull/1418/commits/839843a985e59d2d2a22c01e4ac86919899e2430))
to insert
`<buildArg>--trace-class-initialization=com.google.api.client.util.Base64</buildArg>`
element.

With the argument, the command would print the reference trace to the
problematic class. For example:

```
Error: Classes that should be initialized at run time got initialized during image building:
 com.google.api.client.util.Base64 was unintentionally initialized at build time. com.google.cloud.storage.contrib.nio.CloudStorageFileSystemProvider caused initialization of this class with the following trace:
    at com.google.api.client.util.Base64.<clinit>(Base64.java:33)
    at com.google.api.client.util.PemReader.readNextSection(PemReader.java:99)

(... omit ...)

    at com.google.cloud.storage.contrib.nio.StorageOptionsUtil.<clinit>(StorageOptionsUtil.java:40)
    at com.google.cloud.storage.contrib.nio.CloudStorageFileSystemProvider.<clinit>(CloudStorageFileSystemProvider.java:107)
```

The trace tells the references to the problematic class.

The solution for the error is to explicitly specify the class to be initialized
at image build time. Example:
[java-storage-nio native image test failure fix in June 2024](https://github.com/googleapis/java-storage-nio/pull/1414/commits/9c8451edefc82377c017e6f5b5f5d729a8416a61)

References:

-   [oracle/graal#5134: Providers loaded by FileSystemProvider not handled
    correctly in native image](https://github.com/oracle/graal/issues/5134)
-   [spring-cloud/spring-cloud-config: AOT and Native Image Support](https://github.com/spring-cloud/spring-cloud-config/blob/main/docs/modules/ROOT/pages/server/aot-and-native-image-support.adoc)

### `ClassNotFoundError` when running the tests using the native image

If you managed to build the image, then at native test runtime you may found a
`ClassNotFoundError`. This may occur when a dependency is declared, for example,
as `<scope>provided</scope>`, which means that the jar will be available at
compile time only and will not be embedded in the native image.

Our mature setup counts with the `-Pnative-deps` profile which directs to
include compile and embed certain jars in the native image. Please try
`-Pnative-deps` to make the dependency to be included in the native image.

If we still get the missing class message, consider updating the `native-deps`
profile in your repo (e.g.
[google-http-java-client/google-http-client-test](https://github.com/googleapis/google-http-java-client/blob/3ee8bb6380ee22a8a6eb6e5048c603fbdb9ee320/google-http-client-test/pom.xml#L68-L74)).

### `Value not yet available for AnalysisField<...>`

This usually means that reflection config is not available for `<...>`. Please
find the necessary reflection config
[using the tracing agent](http://go/cloud-java-native-image#Tips). If the
reflection config does not help, consider initializing `<...>` at build time by
adding `--initialize-at-build-time=<...>` in
`src/main/resources/META-INF/native-image/<group_id>/<artifact_id>/nativa-image.properties`.
Note that build-time initialization should be considered as a last resort.

### Tips

*   Use the
    [trace agent](https://www.graalvm.org/22.0/reference-manual/native-image/Agent/)
    if it is not easy to figure out how many configurations we need.

    In order to activate the tracing agent using the `native-maven-plugin`, you
    can run your maven command with `-Dagent=true` (e.g. `mvn test -Pnative ...
    -Dagent=true`. The agent will produce the configuration files in your
    **module's** `target/native/agent-output/test/` folder. Note that since
    GraalVM for JDK 23, the agent will produce a single file
    `reachability-metadata.json`, which is basically a bundle of the formerly
    separated files (e.g. this will have a `reflect` section containing the
    equivalent `reflect-config.json` contents).

    *   The trace agent may include entries in its `reflect-config.json` output
        that should be considered for removal:

        *   Test classes and dependencies such as `**/IT*.java` and
            `org.junit.*`
        *   Classes known to already be covered in other configurations

    *   The trace agent's `reflect-config.json` should be placed in the
        project's
        `<resources>/META-INF/native-image/<groupId>/<artifactId>/reflect-config.json`.

        Example:
        `gax-java/gax/src/main/resources/META-INF/native-image/com.google.api/gax/reflect-config.json`

        Note that,
        [since GraalVM for JDK 23](https://github.com/oracle/graal/commit/74b100c84219835cb040040fa78c1ba65e779a02)
        , the `reachability-metadata.json` file entry for reflection contains
        entries of the form

        ```
        {
            "type": com.google.api.gax.[...].Example
        }
        ```

        In this case, we must move all these entries to a separate
        `reflect-config.json`. The key `"type"` should be replaced by `"name"`.

    *   If a project's integration tests cannot be run locally, consider using
        the existing continuous integration build infrastructure. Modify the
        build script in a PR (for most projects needing infrastructure, this is
        in its `.kokoro` root folder) to invoke the trace agent, and `cat` the
        output `reflect-config.json`, and copy it from the build results.

        For a multi-module project, this may look similar to:

        ```sh
        # Ensure all modules are available locally
        mvn install -DskipTests -B -ntp
        # Execute ITs with native-image-agent
        cd specific-module
        # This instruction is different for google-http-java-client - see below
        mvn verify -Pnative-tests -Pnative
        # Output the result to the console
        cat native-image-config/reflect-config.json
        ```

    *   In case you are running the tests for google-http-java-client, then use
        the following line to test the modules:

        ```sh
        # Only for google-http-java-client
        mvn verify -Pnative-tests -Pnative -Pnative-deps -DargLine="-agentlib:native-image-agent=config-output-dir=native-image-config"
        ```

*   Create a smaller sample to break down the problem. Sometimes there may be
    incompatibilities at several spots in the application so it’s harder to pin
    down the problem. In such cases, using a small sample to reproduce the issue
    can be extremely helpful as it runs much faster than the actual application.
    Here are a few examples:
    [junit-experiment](https://github.com/mpeddada1/junit-experiment),
    [native-resources-config](https://github.com/mpeddada1/native-resources-config).

*   The GraalVM compiler doesn’t give helpful error messages. Make use of the
    debugger and breakpoints in the IDE to dig through the stack trace. Although
    the debugger doesn’t support Native Image testing mode, it is still helpful
    to understand why and where the code path is failing.