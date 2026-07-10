# Copyright (c) 2025, PostgreSQL Global Development Group

test_dwb_sources = files(
  'test_dwb.c',
)

if host_system == 'windows'
  test_dwb_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_dwb',
    '--FILEDESC', 'test_dwb - test module for the short-lived double write buffer',])
endif

test_dwb = shared_module('test_dwb',
  test_dwb_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_dwb

test_install_data += files(
  'test_dwb.control',
  'test_dwb--1.0.sql',
)

tests += {
  'name': 'test_dwb',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_dwb',
    ],
    'regress_args': ['--temp-config', files('test_dwb.conf')],
    'runningcheck': false,
  },
  'tap': {
    'tests': [
      't/001_dwb.pl'
    ],
  },
}
