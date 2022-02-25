include_directories(./cpp)

link_libraries(tables layers core_object)

add_library(fz SHARED cpp/fz.cpp)
