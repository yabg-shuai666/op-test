include_directories(./cpp)

link_libraries(tables layers core_object)

add_library(gbmtools SHARED cpp/gbm.cpp)
