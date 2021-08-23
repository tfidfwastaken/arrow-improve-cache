# Notes

Add these for mac in arrow-improve-cache/cpp/src/gandiva/jni:
```
set(JAVA_AWT_LIBRARY "$JAVA_HOME/include")
set(JAVA_JVM_LIBRARY "$JAVA_HOME/include")
set(JAVA_INCLUDE_PATH2 "$JAVA_HOME/include")
set(JAVA_AWT_INCLUDE_PATH "$JAVA_HOME/include")
```

To get arrow-ipc-read-write to pass:
```
# Set in bashrc/zshrc: (A better way prolly exists)
ARROW_TEST_DATA="$HOME/arrow-improve-cache/testing/data"

# in project root run:
git submodule update --init testing
```

benchbuild:
```
mkdir benchbuild && cd benchbuild
cmake -DCMAKE_BUILD_TYPE=Debug -DARROW_BUILD_TESTS=ON -DARROW_BUILD_BENCHMARKS=ON -DARROW_GANDIVA=ON -DARROW_GANDIVA_JAVA=ON -GNinja ..
ninja
```

To build Gandiva Java: (TODO: Does not work yet)
```
mvn install -pl gandiva -P arrow-jni -am -Darrow.cpp.build.dir=../cpp/debug/debug
```

NOTE: If you are using JDK 16, errorprone will be, well, error prone.  
Fix it by editing pom.xml by bumping errorprone to `2.6.0`.
