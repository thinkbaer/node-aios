cmake_minimum_required(VERSION 3.1)
project(aios-sockit VERSION 1.0.0)

file(GLOB SOURCE_FILES "src/*.cc" "src/*.h")
add_library(${PROJECT_NAME} SHARED ${SOURCE_FILES})

# Gives our library file a .node extension without any "lib" prefix
set_target_properties(${PROJECT_NAME} PROPERTIES PREFIX "" SUFFIX ".node")

# Essential include files to build a node addon,
# You should add this line in every CMake.js based project
target_include_directories(${PROJECT_NAME} PRIVATE ${CMAKE_JS_INC})

# Essential library files to link to a node addon
# You should add this line in every CMake.js based project
target_link_libraries(${PROJECT_NAME} ${CMAKE_JS_LIB})

include(NodeJS.cmake)
nodejs_init()

# message(WARNING ${CMAKE_JS_LIB})
target_include_directories(${PROJECT_NAME} PUBLIC cmake-build-debug/node/v10.15.2/include)
target_include_directories(${PROJECT_NAME} PUBLIC node_modules/nan)
# add_library(node  ${NODEJS_LIBRARIES})


