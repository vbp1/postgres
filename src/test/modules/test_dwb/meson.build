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
    'env': {
      'enable_injection_points': get_option('injection_points') ? 'yes' : 'no',
    },
    'tests': [
      't/001_dwb.pl',
      't/002_flushbuffer.pl',
      't/003_backpressure.pl',
      't/004_retire_paths.pl',
      't/005_standby.pl',
      't/006_backup.pl',
      't/007_rewind.pl',
      't/008_modes.pl',
      't/009_fpw_transition.pl',
      't/010_recovery.pl',
      't/011_geometry_recovery.pl',
      't/012_apply_crafted.pl',
      't/013_backup_start_point.pl',
      't/014_pg_upgrade.pl',
      't/015_vectored_flush.pl',
      't/016_bgwriter_bin.pl',
      't/017_syncfs_retire.pl',
      't/018_cleaners.pl',
    ],
  },
}
