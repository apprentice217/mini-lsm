cmake_minimum_required(VERSION 3.10)
project(MiniLevelDB CXX)

# 提升至 C++17，确保与 Mini-S3 步调一致
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# 保持严苛警告，加入 -fPIC 确保静态库在被其他动态库链接时不会报错
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror=return-type -g")

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


# 注册测试用例，方便以后使用 make test 或 ctest 批量运行
add_test(NAME db_bench COMMAND db_bench)
add_test(NAME db_correctness COMMAND db_correctness)