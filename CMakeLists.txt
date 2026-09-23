cmake_minimum_required(VERSION 3.10)
project(MiniLevelDB CXX)

# 提升至 C++17，确保与 Mini-S3 步调一致
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# 保持严苛警告，加入 -fPIC 确保静态库在被其他动态库链接时不会报错
if(MSVC)
    # MSVC 风格的警告与调试信息
    # /W4      ≈ -Wall -Wextra
    # /we4715  把“并非所有控制路径都返回值”当成错误，≈ -Werror=return-type
    # /Zi      生成调试信息，≈ -g
    add_compile_options(/W4 /we4715 /Zi)
else()
    # GCC / Clang 风格
    add_compile_options(-Wall -Wextra -Werror=return-type -g)
endif()

find_package(Threads REQUIRED)

# 自动搜索实现文件
file(GLOB MINIDB_SOURCES "src/*.cpp")

# 构建静态库
add_library(minidb STATIC ${MINIDB_SOURCES})

# 【核心改进】：显式定义头文件包含路径并向上传播
# 这意味着任何链接了 minidb 的目标会自动获得 include 目录的访问权
target_include_directories(minidb PUBLIC ${PROJECT_SOURCE_DIR}/include)

target_link_libraries(minidb PUBLIC Threads::Threads)

# ==========================================================
# 2. 构建测试程序
# ==========================================================
# 开启 CTest 测试支持
enable_testing()

# 编译 db_bench测试

add_executable(db_bench tests/db_bench.cpp)

target_link_libraries(db_bench minidb)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Unknown")
endif()

execute_process(
    COMMAND git rev-parse --short HEAD
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    OUTPUT_VARIABLE MINIDB_GIT_HASH
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
)

if(NOT MINIDB_GIT_HASH)
    set(MINIDB_GIT_HASH "unknown")
endif()

target_compile_definitions(db_bench PRIVATE
    MINIDB_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
    MINIDB_GIT_HASH="${MINIDB_GIT_HASH}"
)

add_executable(db_correctness tests/db_correctness.cpp)
target_link_libraries(db_correctness minidb)

add_executable(db_bench_mt tests/db_bench_mt.cpp)
target_link_libraries(db_bench_mt minidb)

add_executable(memtable_ds_bench tests/memtable_ds_bench.cpp)
target_link_libraries(memtable_ds_bench minidb)

add_executable(compaction_ab_bench tests/compaction_ab_bench.cpp)
target_link_libraries(compaction_ab_bench minidb)


# 注册测试用例，方便以后使用 make test 或 ctest 批量运行
add_test(NAME db_bench COMMAND db_bench)
add_test(NAME db_correctness COMMAND db_correctness)
add_test(NAME db_bench_mt COMMAND db_bench_mt)
add_test(NAME memtable_ds_bench COMMAND memtable_ds_bench)
add_test(NAME compaction_ab_bench COMMAND compaction_ab_bench)
# Linux syscall wrappers cover durability ordering and injected fsync failures.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_executable(storage_safety_test tests/storage_safety_test.cpp)
    target_link_libraries(storage_safety_test minidb "-Wl,--wrap=fsync" "-Wl,--wrap=rename")
    add_test(NAME storage_safety_test COMMAND storage_safety_test)
endif()
