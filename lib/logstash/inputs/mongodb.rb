# encoding: utf-8

require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/timestamp"
require "stud/interval"
require "socket" # for Socket.gethostname
require "json"
require "mongo"

include Mongo

class LogStash::Inputs::MongoDB < LogStash::Inputs::Base
  config_name "mongodb"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain"

  # Example URI: mongodb://mydb.host:27017/mydbname?ssl=true
  config :uri, :validate => :string, :required => true

  # The directory that will contain the sqlite database file.
  config :last_run_storage_path, :validate => :string, :required => true

  # Any table to exclude by name
  config :exclude_tables, :validate => :array, :default => []

  config :batch_size, :validate => :number, :default => 30

  config :since_table, :validate => :string, :default => "logstash_since"

  # This allows you to select the column you would like to sort on
  config :sort_on, :validate => :string, :default => "_id"

  # This allows to retrieve only the selected fields
  config :projection, :validate => :string, :default => nil

  # The collection to use. Is turned into a regex so 'events' will match 'events_20150227'
  # Example collection: events_20150227 or events_
  config :collection, :validate => :string, :required => true

  # This allows you to select the method you would like to use to parse your data
  config :parse_method, :validate => :string, :default => 'flatten'

  # If not flattening you can dig to flatten select fields
  config :dig_fields, :validate => :array, :default => []

  # This is the second level of hash flattening
  config :dig_dig_fields, :validate => :array, :default => []

  # If true, store the @timestamp field in mongodb as an ISODate type instead
  # of an ISO8601 string.  For more information about this, see
  # http://www.mongodb.org/display/DOCS/Dates
  config :isodate, :validate => :boolean, :default => false

  # Number of seconds to wait after failure before retrying
  config :retry_delay, :validate => :number, :default => 3, :required => false

  # If true, an "_id" field will be added to the document before insertion.
  # The "_id" field will use the timestamp of the event and overwrite an existing
  # "_id" field in the event.
  config :generateId, :validate => :boolean, :default => false

  config :unpack_mongo_id, :validate => :boolean, :default => false

  # The message string to use in the event. unused
  config :message, :validate => :string, :default => "Default message..."

  # Set how frequently messages should be sent.
  # The default, `1`, means send a message every second.
  config :interval, :validate => :number, :default => 1

  # configurable delay between batches, when nothing comes in, 
  # delay is *2, until delay_max
  config :delay, :validate => :number, :default => 5
  config :delay_max, :validate => :number, :default => 300

  # the query to MongoDB, to which we will add the time field selection
  # this is MongoShell like. Dunno how to pass Dates here......
  config :query, :validate => :string, :required => true

  config :start_window, :validate => :string, :default => nil
  config :end_window, :validate => :string, :default => nil

  SINCE_TABLE = :since_table

  public
  def init_placeholder_table()
    begin
      @sqlitedb.create_table "#{SINCE_TABLE}" do
        String :table
        Int :place
        DateTime :glide_start
      end
    rescue
      @logger.debug("since table already exists")
    end
  end

  public
  def init_placeholder(mongodb, mongo_collection_name)
    @logger.debug("init placeholder for #{SINCE_TABLE}_#{mongo_collection_name}")
    since = @sqlitedb[SINCE_TABLE]
    first_entry_id = 0
    since.insert(:table => "#{SINCE_TABLE}_#{mongo_collection_name}", :place => first_entry_id, :glide_start => @start_window_time)
    @logger.info("init placeholder for #{SINCE_TABLE}_#{mongo_collection_name}: #{first_entry_id}")
    return @start_window_time
  end

  public
  def get_placeholder(mongodb, mongo_collection_name)
    since = @sqlitedb[SINCE_TABLE]
    x = since.where(:table => "#{SINCE_TABLE}_#{mongo_collection_name}")
    if x[:glide_start].nil?
      glide_start = init_placeholder(mongodb, mongo_collection_name)
      @logger.debug("FIRST DATE placeholder for #{mongo_collection_name} is #{glide_start}")
      return glide_start
    else
      @logger.debug("placeholder already exists, it is #{x[:glide_start]}")
      @logger.debug("Place holder is of type "+x[:glide_start][:glide_start].class.to_s)
      new_start = x[:glide_start][:glide_start]
      if ! (new_start.is_a? DateTime)
        begin
          begin
            @logger.debug(" Parsing 1")
            new_start = DateTime.strptime(x[:glide_start][:glide_start], "%Y-%m-%d %H:%M:%S.%L")
          rescue Exception => e
            new_start = nil
          end
          if new_start.nil?
            begin
              @logger.debug(" Parsing 2")
              new_start = DateTime.strptime(x[:glide_start][:glide_start], "%Y-%m-%dT%H:%M:%SZ")
            rescue Exception => e
              new_start = nil
            end
          end
          if new_start.nil?
            begin
              @logger.debug(" Parsing 3")
              new_start = DateTime.strptime(x[:glide_start][:glide_start], "%Y-%m-%d")
            rescue Exception => e
              new_start = nil
            end
          end
          if new_start.nil?
            begin
              new_start = DateTime.strptime(x[:glide_start][:glide_start], "%d/%m/%Y")
            rescue Exception => e
              new_start = nil
            end
          end
        end
      end
      return new_start
    end
  end

  public
  def update_placeholder(mongo_collection_name, place)
    #@logger.debug("updating placeholder for #{SINCE_TABLE}_#{mongo_collection_name} to #{place}")
    since = @sqlitedb[SINCE_TABLE]
    @logger.debug("new placeholder #{place} of type "+place.class.to_s)
    since.where(:table => "#{SINCE_TABLE}_#{mongo_collection_name}").update(:glide_start => place)
  end

  public
  def get_all_tables()
    return @mongodb.collection_names
  end

  public
  def get_collection_names(collection)
    collection_names = []
    @mongodb.collection_names.each do |coll|
      if /#{collection}/ =~ coll
        collection_names.push(coll)
        @logger.debug("Added #{coll} to the collection list as it matches our collection search")
      end
    end
    return collection_names
  end

  private
  def build_mongo_query(user_query, glide_start)
      querymongo = { :$and => [ {@sort_on => {:$gte => @start_window_time}},
                                {@sort_on => {:$gte => glide_start}},
                                {@sort_on => {:$lt => @end_window_time}},
                                user_query ]}
    @logger.debug("Built mongo query "+querymongo.to_s)
    return querymongo
  end

  public
  def get_cursor_for_collection(mongodb, mongo_collection_name, sort_col, glide_start, batch_size, user_query)
    @logger.debug("querying mongo collection name "+mongo_collection_name.to_s)
    @logger.debug("querying with sort column "+sort_col+ " of class "+sort_col.class.to_s)
    collection = mongodb.collection(mongo_collection_name)
    # Need to make this sort by date in object id then get the first of the series
    # db.events_20150320.find().limit(1).sort({ts:1})
    @logger.debug("querying mongo collection "+collection.to_s)
    querymongo = build_mongo_query(user_query, glide_start)
    @logger.debug("querying mongo for "+batch_size.to_s+" documents after date "+glide_start.to_s)
    return collection.find(querymongo, :projection => @user_projection).sort(sort_col => 1).limit(batch_size)
  end

  public
  def update_watched_collections(mongodb, collection)
    collections = get_collection_names(collection)
    collection_data = {}
    collections.each do |my_collection|
      last_id = get_placeholder(mongodb, my_collection)
      if !collection_data[my_collection]
        collection_data[my_collection] = { :name => my_collection, :last_id => last_id }
      end
    end
    return collection_data
  end

  public
  def register
    require "jdbc/sqlite3"
    require "sequel"
    conn = Mongo::Client.new(@uri)

    @host = Socket.gethostname
    @logger.info("Registering MongoDB input")

    @mongodb = conn.database
    @sqlitedb = Sequel.connect("jdbc:sqlite:#{@last_run_storage_path}")

  end # def register

  class BSON::OrderedHash
    def to_h
      inject({}) { |acc, element| k,v = element; acc[k] = (if v.class == BSON::OrderedHash then v.to_h else v end); acc }
    end

    def to_json
      JSON.parse(self.to_h.to_json, :allow_nan => true)
    end
  end

  def flatten(my_hash)
    new_hash = {}
    #@logger.debug("Raw Hash: #{my_hash}")
    if my_hash.respond_to? :each
      my_hash.each do |k1,v1|
        #@logger.debug("Flattening "+k1.to_s+" value "+v1.to_s)
        if v1.is_a?(Hash)
          #@logger.debug("   is a hash")
          v1.each do |k2,v2|
            if v2.is_a?(Hash)
              #@logger.debug("Found a nested hash "+k2.to_s+"   "+v2.to_s)
              result = flatten(v2)
              result.each do |k3,v3|
                new_hash[k1.to_s+"_"+k2.to_s+"_"+k3.to_s] = v3
              end
              # puts "result: "+result.to_s+" k2: "+k2.to_s+" v2: "+v2.to_s
            else
              #@logger.debug("Found a nested but not hash "+k2+"   "+v2)
              new_hash[k1.to_s+"_"+k2.to_s] = v2
            end
          end
        else
          #@logger.debug("   is not a hash")
          #@logger.debug("     to string key "+k1.to_s)
          #@logger.debug("     to string value "+v1.to_s)
          # puts "Key: "+k1.to_s+" is not a hash"
          new_hash[k1.to_s] = v1
          #@logger.debug("OK")
        end
      end
    else
      @logger.debug("Flatten [ERROR]: hash did not respond to :each")
    end
    #@logger.debug("Flattened Hash: #{new_hash}")
    return new_hash
  end

  def run(queue)
    sleep_min = @delay
    sleep_max = @delay_max
    sleeptime = sleep_min
    batch_size = @batch_size


    if @projection.nil?
      @user_projection = {}
    else
      @user_projection = JSON.parse(@projection)
    end

    @user_query = JSON.parse(@query)
    if ! (@user_query.is_a? Hash )
        @logger.error("query must be a valid mongo shell query: "+@user_query)
    end

    @start_window_time = DateTime.strptime(@start_window,"%Y-%m-%d %H:%M:%S")
    @logger.info("start of window "+@start_window_time.to_s)

    @end_window_time = DateTime.strptime(@end_window,"%Y-%m-%d %H:%M:%S")
    @logger.info("end of window date time "+@end_window_time.to_s)
    @logger.info("end of window time      "+@end_window_time.to_time.to_s)
    @logger.info("end of window time      "+@end_window_time.to_time.to_i.to_s)

    init_placeholder_table()

    # Should check to see if there are new matching tables at a predefined interval or on some trigger
    @collection_data = update_watched_collections(@mongodb, @collection)

    @logger.debug("Tailing MongoDB")
    @logger.debug("Collection data is: #{@collection_data}")

    perfFlatten = 0.0
    perfDataMove = 0.0
    perfSql = 0.0
    perfPost = 0.0
    startPerf = Time.now

    while true && !stop?
      begin
        @collection_data.each do |index, collection|
          collection_name = collection[:name]
          @logger.debug("collection_data is: #{@collection_data}")
          last_id = @collection_data[index][:last_id]
          @logger.debug("last_id is #{last_id}   of class "+last_id.class.to_s, :index => index, :collection => collection_name)

          startPerf = Time.now;
          cursor = get_cursor_for_collection(@mongodb, collection_name, @sort_on, last_id, batch_size, @user_query)
          measurePerf = Time.now - startPerf
          @logger.debug("Cursor get "+measurePerf.to_s+ "  s")

          counter = 0
          startPerf = Time.now;
          cursor.each do |doc|
            sleeptime = sleep_min
            event = LogStash::Event.new("host" => @host)
            decorate(event)
            log_entry = doc.to_h.to_s
            event.set("log_entry",log_entry.force_encoding(Encoding::UTF_8))
            event.set("mongo_id",doc['_id'].to_s)
            @logger.debug("NEW DOCUMENT")
            @logger.debug("mongo_id: "+doc['_id'].to_s)
            @logger.debug("doc sorted field : "+doc[@sort_on].to_s)
            # @logger.debug("EVENT looks like: "+event.to_s)
            #@logger.debug("Sent message: "+doc.to_h.to_s)
            # Extract the HOST_ID and PID from the MongoDB BSON::ObjectID
            if @unpack_mongo_id
              doc_hex_bytes = doc['_id'].to_s.each_char.each_slice(2).map {|b| b.join.to_i(16) }
              doc_obj_bin = doc_hex_bytes.pack("C*").unpack("a4 a3 a2 a3")
              host_id = doc_obj_bin[1].unpack("S")
              process_id = doc_obj_bin[2].unpack("S")
              event.set('host_id',host_id.first.to_i)
              event.set('process_id',process_id.first.to_i)
            end

            if @parse_method == 'flatten'
              # Flatten the JSON so that the data is usable in Kibana
              startFlatten = Time.now
              flat_doc = flatten(doc)
              perfFlatten = perfFlatten + (Time.now - startFlatten).to_f
              # Check for different types of expected values and add them to the event
              if flat_doc['info_message'] && (flat_doc['info_message']  =~ /collection stats: .+/)
                # Some custom stuff I'm having to do to fix formatting in past logs...
                sub_value = flat_doc['info_message'].sub("collection stats: ", "")
                JSON.parse(sub_value).each do |k1,v1|
                  flat_doc["collection_stats_#{k1.to_s}"] = v1
                end
              end

              startDataMove =Time.now
              flat_doc.each do |k,v|
                # Check for an integer
                #@logger.debug("key: #{k.to_s} of type "+v.class.to_s+" value: #{v.to_s}")
                if v.is_a? Numeric
                  event.set(k.to_s,v)
                elsif v.is_a? Time or v.is_a? DateTime
                  event.set(k.to_s,v.iso8601)

                elsif v.is_a? String
                  if v == "NaN"
                    event.set(k.to_s, Float::NAN)
                  elsif /\A[-+]?\d+[.][\d]+\z/ == v
                    event.set(k.to_s, v.to_f)
                  elsif (/\A[-+]?\d+\z/ === v) || (v.is_a? Integer)
                    event.set(k.to_s, v.to_i)
                  else
                    event.set(k.to_s, v)
                  end
                elsif v.is_a? Array
                  event.set(k.to_s, v)
                else
                  if k.to_s  == "_id" || k.to_s == "tags"
                    event.set(k.to_s, v.to_s )
                  elsif (k.to_s == "tags") && (v.is_a? Array)
                    event.set('tags',v)
                  else
                    event.set(k.to_s,v)
                  end

                end
              end

              perfDataMove = perfDataMove + (Time.now - startDataMove).to_f

            elsif @parse_method == 'dig'
              # Dig into the JSON and flatten select elements
              doc.each do |k, v|
                if k != "_id"
                  if (@dig_fields.include? k) && (v.respond_to? :each)
                    v.each do |kk, vv|
                      if (@dig_dig_fields.include? kk) && (vv.respond_to? :each)
                        vv.each do |kkk, vvv|
                          if /\A[-+]?\d+\z/ === vvv
                            event.set("#{k}_#{kk}_#{kkk}",vvv.to_i)
                          else
                            event.set("#{k}_#{kk}_#{kkk}", vvv.to_s)
                          end
                        end
                      else
                        if /\A[-+]?\d+\z/ === vv
                          event.set("#{k}_#{kk}", vv.to_i)
                        else
                          event.set("#{k}_#{kk}",vv.to_s)
                        end
                      end
                    end
                  else
                    if /\A[-+]?\d+\z/ === v
                      event.set(k,v.to_i)
                    else
                      event.set(k,v.to_s)
                    end
                  end
                end
              end
            elsif @parse_method == 'simple'
              doc.each do |k, v|
                  if v.is_a? Numeric
                    event.set(k, v.abs)
                  elsif v.is_a? Array
                    event.set(k, v)
                  elsif v == "NaN"
                    event.set(k, Float::NAN)
                  else
                    event.set(k, v.to_s)
                  end
              end
            elsif @parse_method == 'raw'
              doc.each do |k,v|
                if v.is_a? Numeric
                  event.set(k, v.abs)
                elsif v.is_a? Array
                  new_array = []
                  v.each do |hash|
                    new_array << hash
                  end
                  event.set(k, new_array.to_json)
                elsif v == "NaN"
                  event.set(k, Float::NAN)
                else
                  event.set(k, v.to_s)
                end
              end
            end

            event.set('start_window',@start_window)
            event.set('end_window',@end_window)

            startPost = Time.now
            queue << event
            perfPost = perfPost + (Time.now - startPost).to_f

            @collection_data[index][:last_id] =  event.get('localEnd')

          end
          startSql = Time.now
          # Store the last seen end date for this collection (in case of interrupt)
          update_placeholder(collection_name, @collection_data[index][:last_id])
          perfSql = perfSql + ( Time.now - startSql).to_f

          @logger.debug(" Perf flatten "+perfFlatten.to_s)
          @logger.debug(" Perf data move  "+perfDataMove.to_s)
          @logger.debug(" Perf post  "+perfPost.to_s)
          @logger.debug(" Perf sql  "+perfSql.to_s)
        end
        measurePerf = Time.now - startPerf
        @logger.debug("Perf cursor loop "+measurePerf.to_s+ "  s")

        @logger.debug("Updating watch collections")
        @collection_data = update_watched_collections(@mongodb, @collection)

        # nothing found in that iteration
        # sleep a bit
        @logger.debug("No new rows. Sleeping.", :time => sleeptime)
        sleeptime = [sleeptime * 2, sleep_max].min
        sleep(sleeptime)
      rescue => e
        @logger.warn('MongoDB Input threw an exception, restarting', :exception => e)
      end
    end
  end # def run

  def close
    # tidy up on shutdown
    @logger.info('MongoDB Input plugin exiting on shutdown request...')
    @collection_data.each do |index, collection|
      collection_name = collection[:name]
      update_placeholder(collection_name, @collection_data[index][:last_id])
    end
  end

end # class LogStash::Inputs::Example
