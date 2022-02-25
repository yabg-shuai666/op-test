include_directories(.)

link_libraries(tables layers core_object)

add_library(hctools SHARED ./hctools.cpp)
