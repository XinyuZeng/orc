mkdir build-debug
cd build-debug
cmake .. -DCMAKE_BUILD_TYPE=DEBUG -DBUILD_JAVA=OFF -DSTOP_BUILD_ON_WARNING=OFF
make package
make test-out
