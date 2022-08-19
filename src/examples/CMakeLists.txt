set(CMAKE_CXX_STANDARD 11)
set(CMAKE_BUILD_TYPE Debug)

# For develop on Mac
include_directories(${PROJECT_SOURCE_DIR}/src/include)

# On Centos
#include_directories(/usr/include)
#link_libraries(/usr/lib64)

add_executable(ceph_client ceph_client.cc)
target_link_libraries(ceph_client librados)