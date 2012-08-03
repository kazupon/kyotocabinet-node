{
    'variables': {
        'kyotocabinet_shared_include_dir': '<(module_root_dir)/deps/kyotocabinet',
        'kyotocabinet_shared_library': '<(module_root_dir)/deps/kyotocabinet/libkyotocabinet.a'
    }, 
    'target_defaults': {
        # HACK:S resolve exception throw compile error !!
        'cflags!': [ '-fno-rtti', '-fno-exceptions' ],
        'cflags_cc!': [ '-fno-rtti', '-fno-exceptions' ],
        'cflags': [ '-fexceptions' ],
        'cflags_cc': [ '-fexceptions' ],
        # HACK:E resolve exception throw compile error !!
        'configurations': {
            'Debug': {
                'defines': [ 'DEBUG', '_DEBUG' ]
            },
            'Release': {
                'defines': [ 'NDEBUG' ]
            }
        },
    },
    'targets': [{
        'target_name': 'kyotocabinet',
        'dependencies': [
            'kyotocabinet_core'
        ],
        'sources': [
            './src/debug.c',
            './src/utils.cc',
            './src/async.cc',
            './src/error_wrap.cc',
            './src/visitor_wrap.cc',
            './src/cursor_wrap.cc',
            './src/mapreduce_wrap.cc',
            './src/polydb_wrap.cc',
            './src/kyotocabinet.cc'
        ],
        'libraries': [
            '<(kyotocabinet_shared_library)'
        ], 
        'include_dirs': [
            '<(kyotocabinet_shared_include_dir)'
        ], 
        'cflags': [ '-g', '-O0' ],
        'ldflags': [ '-lz' ],
        'conditions': [[
            'OS == "win"', {
            }
        ], [
            'OS=="linux" or OS=="freebsd" or OS=="openbsd" or OS=="solaris"', {
            }
        ], [
            'OS=="mac"', {
                'xcode_settings': {
                    'GCC_ENABLE_CPP_EXCEPTIONS': 'YES',
                    'GCC_ENABLE_CPP_RTTI': 'YES'
                }
            }
        ]]
    }, {
        'target_name': 'kyotocabinet_core',
        'type': 'none',
        'actions': [{
            'action_name': 'test',
            'inputs': ['<!@(sh kyotocabinet-config.sh)'],
            'outputs': [''],
            'conditions': [[
                'OS=="win"', {
                    'action': [
                        'echo', 'notsupport'
                    ]
                }, {
                    'action': [
                        # run kyotocabinet `make`
                        'sh', 'kyotocabinet-build.sh'
                    ]
                }
            ]]
        }]
    }]
}
