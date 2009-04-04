require 'aws/s3'

class S3Utils

  include AWS::S3
  S3ID  = ""
  S3KEY = ""
  
  def initialize
    puts "connecting..."
    AWS::S3::Base.establish_connection!(
      :access_key_id     => S3ID,
      :secret_access_key => S3KEY
    )
  end

  def copy_bucket(from_bucket_name, to_bucket_name, folder = nil)
    from_bucket = Bucket.find(from_bucket_name)
    to_bucket   = Bucket.find(to_bucket_name)
    
    if from_bucket && to_bucket
      marker      = nil
      new_marker  = "somethingsarebetterleftunsaid"
      
      # bucket.objects returns in batches of 1000 only, need to get more if we have more
      while(new_marker && marker != new_marker) do
        
        marker  = new_marker == "somethingsarebetterleftunsaid" ? nil : new_marker
        files   = bucket_objects(from_bucket, folder, marker)

        files.each do |file|
          copy_between_buckets(from_bucket, to_bucket, file)
        end
                
        begin
          files.last.path =~ /#{folder}\/(.*)/
          new_marker = "#{folder}/#{$1}/"
        rescue
          new_marker = nil
        end
        
      end
    else
      puts "Either #{from_bucket_name} or #{to_bucket_name} does not exist"
      exit
    end
  end

  def copy_bucket_in_batches(from_bucket_name, to_bucket_name, folder_list)
    folder_list.each do |folder|
      copy_bucket(from_bucket_name, to_bucket_name, folder)
    end
  end

  private ###########################################

    def bucket_objects(bucket, folder = nil, marker = nil)
      bucket.objects(:prefix => folder, :marker => marker)
    end
    
    def copy_between_buckets(from_bucket, to_bucket, file)
      from_key = to_key = file.key

      if S3Object.exists?(to_key, to_bucket.name)
        puts "Destination file #{to_bucket.name}.#{to_key} exists. Skipping...\n"
      else
        puts "Copying #{from_bucket.name}.#{from_key} to #{to_bucket.name}.#{to_key}...\n"
        S3Object.copy_bucket_to_bucket(from_bucket, to_bucket, file, :copy_acl => :true)
      end
    end    
end

module AWS
  module S3
    class S3Object
      class << self
        def copy_bucket_to_bucket(from_bucket, to_bucket, file, options = {})
          from_bucket_name= from_bucket.name
          to_bucket_name  = to_bucket.name
          key             = file.key
  
          source_key      = path!(from_bucket_name, key)
          target_key      = path!(to_bucket_name, key)
  
          default_options = {'x-amz-copy-source' => source_key}
  
          begin
            returning put(target_key, default_options) do
              acl(file.key, to_bucket_name, acl(file.key, from_bucket_name)) if options[:copy_acl]
            end
          rescue
            nil
          end
        end
      end
    end
  end
end