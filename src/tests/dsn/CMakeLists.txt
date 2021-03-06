set(MY_PROJ_NAME dsn.tests)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

#  Search mode for source files under CURRENT project directory?
#  "GLOB_RECURSE" for recursive search
#  "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS 
    dsn_meta_server
    dsn_replica_server
    dsn_replication_common
    dsn.block_service.local
    dsn.block_service.fds
    dsn.failure_detector.multimaster
    dsn.failure_detector
    dsn.replication.zookeeper_provider
    ${ZK_MT}
    ${FDS_LIB}
    Poco::Net
    Poco::Foundation
    Poco::NetSSL
    Poco::JSON
    OpenSSL::Crypto
    fmt::fmt
    GTest::GTest
    Threads::Threads
    )

set(MY_BOOST_LIBS Boost::system Boost::filesystem)

set(MY_PROJ_LIB_PATH "")

# Extra files that will be installed
set(MY_BINPLACES
    "${CMAKE_CURRENT_SOURCE_DIR}/run.sh"
    "${CMAKE_CURRENT_SOURCE_DIR}/restart_zookeeper.sh"
    "${CMAKE_CURRENT_SOURCE_DIR}/clear.sh"
    "${CMAKE_CURRENT_SOURCE_DIR}/clear.cmd"
    "${CMAKE_CURRENT_SOURCE_DIR}/config-test.ini"
    "${CMAKE_CURRENT_SOURCE_DIR}/config-whitelist-test.ini"
    "${CMAKE_CURRENT_SOURCE_DIR}/config-whitelist-test-failed.ini"
    "${CMAKE_CURRENT_SOURCE_DIR}/gtest.filter"
)

dsn_add_test()
