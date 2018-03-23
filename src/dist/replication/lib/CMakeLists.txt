set(MY_PROJ_NAME dsn_layer2_stateful_type1)

set(DUPLICATION_SRC duplication/replica_duplication.cpp
                    duplication/replica_stub_duplication.cpp
                    duplication/mutation_duplicator.cpp
                    duplication/duplication_pipeline.cpp
                    duplication/private_log_loader.cpp
                    duplication/mutation_batch.cpp)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC ${DUPLICATION_SRC})

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH "")

set(MY_PROJ_LIBS
    dsn.replication.clientlib
    dsn.failure_detector
    dsn.failure_detector.multimaster
    )

set(MY_PROJ_LIB_PATH "")

# Extra files that will be installed
set(MY_BINPLACES "")

dsn_add_shared_library()
