py.extension_module(
    '_backward',
    '_backward.pyx',
    dependencies: [backward_dep],
    override_options : ['cython_language=cpp'],
)

py.extension_module(
    '_lib',
    '_blocking_impl.cc',
    '_lib.pyx',
    dependencies: [adbc_driver_manager_dep],
    override_options : ['cython_language=cpp'],
)

py.extension_module(
    '_reader',
    '_reader.pyx',
    dependencies: [adbc_driver_manager_dep],
    override_options : ['cython_language=cpp'],
)
