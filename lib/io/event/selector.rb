# frozen_string_literal: true

# Released under the MIT License.
# Copyright, 2021-2026, by Samuel Williams.

require_relative "native"
require_relative "selector/select"
require_relative "debug/selector"

require "etc"

module IO::Event
	# @namespace
	module Selector
		selectors = [:EPoll, :KQueue, :IOCP, :Select]
		BEST = const_get(selectors.find{|name| const_defined?(name)})
		private_constant :BEST
		
		URING_MINIMUM_KERNEL_VERSION = [5, 11].freeze
		private_constant :URING_MINIMUM_KERNEL_VERSION
		
		# The default selector implementation, which is chosen based on the environment and available implementations.
		#
		# @parameter env [Hash] The environment to read configuration from.
		# @returns [Class] The default selector implementation.
		def self.default(env = ENV)
			if name = env["IO_EVENT_SELECTOR"]&.to_sym
				return const_get(name)
			elsif uring_supported?(env)
				return URing
			else
				BEST
			end
		end
		
		def self.uring_supported?(env = ENV)
			return false unless const_defined?(:URing)
			return false unless linux_platform?(env)
			
			if version = linux_kernel_version(env)
				(version <=> URING_MINIMUM_KERNEL_VERSION) >= 0
			end
		end
		
		def self.linux_platform?(env = ENV)
			(env["IO_EVENT_PLATFORM"] || RUBY_PLATFORM).include?("linux")
		end
		
		def self.linux_kernel_version(env = ENV)
			release = env["IO_EVENT_KERNEL_RELEASE"] || Etc.uname[:release]
			
			if match = release.match(/\A(\d+)\.(\d+)/)
				[match[1].to_i, match[2].to_i]
			end
		rescue
			nil
		end
		
		private_class_method :uring_supported?, :linux_platform?, :linux_kernel_version
		
		# Create a new selector instance, according to the best available implementation.
		#
		# @parameter loop [Fiber] The event loop fiber.
		# @parameter env [Hash] The environment to read configuration from.
		# @returns [Selector] The new selector instance.
		def self.new(loop, env = ENV)
			selector = default(env).new(loop)
			
			if debug = env["IO_EVENT_DEBUG_SELECTOR"]
				selector = Debug::Selector.wrap(selector, env)
			end
			
			return selector
		end
	end
end
