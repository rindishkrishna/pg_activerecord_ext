Gem::Specification.new do |s|
  s.name = "pg_activerecord_ext"
  s.version = "0.0.0"
  s.authors = ['Gautam Punhani']
  s.summary = "Gem for providing pipelining functionality in postgres active_record adapter"

  s.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{\A(?:test|spec|features)/}) }
  end
end