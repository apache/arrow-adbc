project(
    'adbc-driver-manager',
    'cython', 'cpp',
    # TODO: determine dynamically https://github.com/mesonbuild/meson-python/issues/159
    version: '1.2.0-SNAPSHOT',
    default_options: [
        'buildtype=release',
        'c_std=c99',
        'warning_level=2',
        'cpp_std=c++17',
    ]
)

backward_dep = dependency('backward-cpp')
adbc_driver_manager_dep = dependency(
    'adbc_driver_manager',
    default_options: ['driver_manager=true']
)

py = import('python').find_installation()

subdir('adbc_driver_manager')
