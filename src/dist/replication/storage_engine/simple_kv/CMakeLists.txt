set(MY_PROJ_NAME dsn.replication.simple_kv)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH "")

set(MY_PROJ_LIBS dsn_replica_server dsn_meta_server dsn_replication_client fmt::fmt)

set(MY_BOOST_LIBS Boost::system Boost::filesystem)

set(MY_PROJ_LIB_PATH "")

set(INI_FILES "")
file(GLOB
    RES_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.ini"
    "${CMAKE_CURRENT_SOURCE_DIR}/*.sh"
    )

# Extra files that will be installed
set(MY_BINPLACES ${RES_FILES})

dsn_add_test()
