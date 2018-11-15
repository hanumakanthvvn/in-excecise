require 'json/stream'
require 'persistdata'

class MyJsonHandler

 CALLBACKS = %w[start_document end_document start_object end_object start_array end_array key value]

  def initialize
     setup_dependencies
     setup_parser_event_hooks
     @db_client = Persistdata.instance   
  end

  def setup_dependencies
    @running = false
    @parent_hash = {}
    @inner_hash = {}
    @current_state = 0
    @array_active = false
    @key = ""
  end

  def setup_parser_event_hooks
    @parser = JSON::Stream::Parser.new
    CALLBACKS.each do |name|
      @parser.send(name, &method(name))
    end
  end

  def start_document  
    p "Started Parsing JSON file at: #{Time.now}" 
  end

  def end_document  
    p "Finished Parsing Json file at: #{Time.now}"
  end

  def start_object 
    if @parent_hash.empty?
      @parent_hash = {}
    else
      @current_state = 1
      @inner_hash = {}
    end
  end 

  def end_object 
    case @current_state
    when 0
      @db_client.insert_data(@parent_hash)
      @parent_hash.clear
    when 1
      @current_state = 0
      @parent_hash.merge!(@inner_hash)
      @inner_hash.clear 
    end
  end

  def start_array 
    if @running
      @array_active = true
      case @current_state
      when 0
        @parent_hash[@key] = []
      when 1
        @inner_hash[@key] = []
      end
    else
      @running = true
    end
  end

  def end_array 
    @array_active = false
  end

  def key(k)
    @key = k
  end

  def value(v)
    if @array_active
      case @current_state
        when 0
          @parent_hash[@key] << v
        when 1
          @inner_hash[@key] << v  
      end
    else
      case @current_state
        when 0
          @parent_hash[@key] = v
        when 1
          @inner_hash[@key] = v  
      end
    end
  end

  def run
    file_pointer = File.open(File.join(File.dirname(__FILE__), 'test.json'))
    begin
      until file_pointer.eof?
       @parser << file_pointer.read(512)
      end
    rescue EOFError
      file_pointer.close
    end
  end

end

handler_obj = MyJsonHandler.new
handler_obj.run
