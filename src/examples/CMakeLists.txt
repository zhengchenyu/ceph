set(CMAKE_CXX_STANDARD 20)
set(CMAKE_BUILD_TYPE Debug)

# For develop on Mac
include_directories(${PROJECT_SOURCE_DIR}/src/include)

# On Centos
#include_directories(/usr/include)
#link_libraries(/usr/lib64)

add_executable(ceph_client ceph_client.cc)
target_link_libraries(ceph_client librados)

add_executable(simple_file_server_msgr_client simple_file_server_msgr_client.cc)
add_executable(simple_file_server_msgr_server simple_file_server_msgr_server.cc)
target_link_libraries(simple_file_server_msgr_client os global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})
target_link_libraries(simple_file_server_msgr_server os global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})

add_executable(cephfs_client cephfs_client.cc)
target_link_libraries(cephfs_client ceph-common cephfs)