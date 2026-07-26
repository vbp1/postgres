
# Copyright (c) 2025, PostgreSQL Global Development Group

# Shared helpers for the test_dwb TAP suite: raw 8 kB block I/O on relation
# files, for damaging and inspecting pages behind the server's back.

package DWBTest;

use strict;
use warnings FATAL => 'all';
use Exporter 'import';

our @EXPORT = qw(read_block write_block);

sub read_block
{
	my ($file, $blkno) = @_;
	my $buf;

	open my $fh, '<:raw', $file or die "could not open $file: $!";
	sysseek($fh, $blkno * 8192, 0) or die "could not seek $file: $!";
	sysread($fh, $buf, 8192) == 8192 or die "short read from $file: $!";
	close $fh;
	return $buf;
}

sub write_block
{
	my ($file, $blkno, $buf) = @_;

	open my $fh, '+<:raw', $file or die "could not open $file: $!";
	sysseek($fh, $blkno * 8192, 0) or die "could not seek $file: $!";
	syswrite($fh, $buf) == length($buf) or die "short write to $file: $!";
	close $fh;
	return;
}

1;
