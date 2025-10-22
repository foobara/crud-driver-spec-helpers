require_relative "version"

Gem::Specification.new do |spec|
  spec.name = "foobara-crud-driver-spec-helpers"
  spec.version = Foobara::CrudDriverSpecHelpers::VERSION
  spec.authors = ["Miles Georgi"]
  spec.email = ["azimux@gmail.com"]

  spec.summary = "Spec helpers for crud driver projects"
  spec.homepage = "https://github.com/foobara/crud-driver-spec-helpers"
  spec.license = "MPL-2.0"
  spec.required_ruby_version = Foobara::CrudDriverSpecHelpers::MINIMUM_RUBY_VERSION

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/main/CHANGELOG.md"

  spec.files = Dir[
    "lib/**/*",
    "src/**/*",
    "LICENSE*.txt",
    "README.md",
    "CHANGELOG.md"
  ]

  spec.add_dependency "base64"
  spec.add_dependency "foobara", ">= 0.1.16", "< 2.0.0"

  spec.require_paths = ["lib"]
  spec.metadata["rubygems_mfa_required"] = "true"
end
