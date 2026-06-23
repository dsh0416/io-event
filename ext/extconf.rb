#!/usr/bin/env ruby
# frozen_string_literal: true

# Released under the MIT License.
# Copyright, 2021-2026, by Samuel Williams.

return if RUBY_DESCRIPTION =~ /jruby/

require "mkmf"
require "rb_sys/mkmf"

features = ENV.fetch("RB_SYS_CARGO_FEATURES", ENV.fetch("CARGO_FEATURES", ""))
features = features.split(/[\s,]+/).reject(&:empty?)

create_rust_makefile("IO_Event") do |config|
	config.features = features
	config.profile = ENV.fetch("RB_SYS_CARGO_PROFILE", ENV.fetch("PROFILE", "release")).to_sym
	config.target_dir = ENV["RB_SYS_CARGO_TARGET_DIR"] || ENV["CARGO_TARGET_DIR"]
end
