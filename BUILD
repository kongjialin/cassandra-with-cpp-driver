cc_library(
    name = 'random_message',    
    srcs = [
        'murmurhash3.cpp',
        'random_message.cpp',
    ],
    deps = [
        '//common/idl:message_thrift',
        '//thirdparty/cass:cassandra_thrift',
        '//thirdparty/gflags:gflags',
    ],
)

cc_library(
    name = 'cass_driver',
    srcs = 'cass_driver.cpp',
    deps = [
        '//common/idl:message_thrift',
        '//storage/driver:cassandra',
        '//storage/uv:uv',
        '//thirdparty/gflags:gflags',
        '#rt',
    ],
)

cc_binary(
    name = 'stress_driver',
    srcs = 'stress_driver.cpp',
    deps = [
        ':random_message',
        '//storage/driver:cassandra',
        '//storage/uv:uv',
        '//common/base:timestamp',
        '//thirdparty/gflags:gflags',
        '//thirdparty/boost:boost_system',
        '//thirdparty/boost:boost_thread',
        '#rt',
    ],
)

cc_binary(
   name = 'cass_driver_example',
   srcs = 'cass_driver_example.cpp',
   deps = [
        ':cass_driver',
        '//thirdparty/boost:boost_chrono',
        '//thirdparty/boost:boost_system',
        '//thirdparty/boost:boost_thread',
   ],
)

cc_binary(
   name = 'cass_stress',
   srcs = 'cass_stress.cpp',
   deps = [
       ':cass_driver',
       ':random_message',
       '//common/base:timestamp',
       '//common/idl:message_thrift',
       '//thirdparty/gflags:gflags',
       '//thirdparty/boost:boost_chrono',
       '//thirdparty/boost:boost_system',
       '//thirdparty/boost:boost_thread',
       '#rt',
   ],
)

cc_test(
  name = 'cass_driver_test',
  srcs = 'cass_driver_test.cpp',
  deps = [
      ':cass_driver',
      '//thirdparty/boost:boost_chrono',
      '//thirdparty/boost:boost_system',
      '//thirdparty/boost:boost_thread',
      '//thirdparty/glog:glog',
  ],
)

cc_binary(
    name = 'cass_yaml_config',
    srcs = 'cass_yaml_config.cpp',
    deps = [
        '//storage/yamlcpp:yaml',
        '//thirdparty/gflags:gflags',
    ],
    incs = [
        '.',
        './yamlcpp',
        './yamlcpp/yaml-cpp',
    ]
)

