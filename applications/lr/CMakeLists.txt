include_directories(.)

link_libraries(tables layers core_object)

add_library(lrtools SHARED ./lr.cpp)
