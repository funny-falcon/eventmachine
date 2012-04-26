require 'em_test_helper'
require 'tempfile'

class TestFileWatch < Test::Unit::TestCase
  if windows?
    def test_watch_file_raises_unsupported_error
      assert_raises(EM::Unsupported) do
        EM.run do
          file = Tempfile.new("fake_file")
          EM.watch_file(file.path)
        end
      end
    end
  elsif EM.respond_to? :watch_filename
    module FileWatcher
      def file_modified
        $modified = true
      end
      def file_deleted
        $deleted = true
      end
      def unbind
        $unbind = true
        EM.stop
      end
    end

    module DirWatcherINotify
      def file_modified(file = nil)
        $modified << file
      end
      def unbind
        $unbind = true
        EM.stop
      end
    end

    def setup
      $modified = []
      $deleted = nil
      $unbind = nil
      EM.kqueue = true if EM.kqueue?
    end

    def teardown
      EM.kqueue = false if EM.kqueue?
    end

    def test_events
      EM.run{
        file = Tempfile.new('em-watch')
        $tmp_path = file.path

        # watch it
        watch = EM.watch_file(file.path, FileWatcher)
        $path = watch.path

        # modify it
        File.open(file.path, 'w'){ |f| f.puts 'hi' }

        # delete it
        EM.add_timer(0.01){ file.close; file.delete }
      }

      assert_equal($path, $tmp_path)
      assert($modified)
      assert($deleted)
      assert($unbind)
    end

    if linux?
      def test_directory
        path = File.expand_path('../test_watch_dir', __FILE__)
        EM.run {
          Dir.mkdir(path)

          watch = EM.watch_file(path, DirWatcherINotify)

          file = File.join(path, 'test_file')
          file1 = File.join(path, 'test_file1')
          File.open(file, 'w') do end

          EM.add_timer(0.01){ 
            File.rename(file, file1)
            EM.add_timer(0.01){
              File.unlink(file1)
              EM.add_timer(0.01) {
                Dir.rmdir(path)
              }
            }
          }
        }
        assert_equal(%w{test_file test_file test_file1 test_file1}, $modified)
      rescue
        Dir[path+'/*'].each{|f| File.unlink(f)}
        Dir.rmdir(path)
        raise
      end
    end
  else
    warn "EM.watch_file not implemented, skipping tests in #{__FILE__}"

    # Because some rubies will complain if a TestCase class has no tests
    def test_em_watch_file_unsupported
      assert true
    end
  end
end
