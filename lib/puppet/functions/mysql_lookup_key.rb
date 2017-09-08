Puppet::Functions.create_function(:mysql_lookup_key) do

  #$use_jdbc = defined?(JRUBY_VERSION) ? true : false 
  $use_jdbc = true
  Puppet.debug("Using jdbc = #{$use_jdbc}")
  if $use_jdbc
    begin
      require 'jdbc/mysql'
      require 'java'
    rescue LoadError => e
      raise Puppet::DataBinding::LookupError, "Must install jdbc-mysql gem to use hiera-mysql"
    end
  else
    begin
      require 'mysql'
    rescue LoadError => e
      raise Puppet::DataBinding::LookupError, "Must install mysql gem to use hiera-mysql"
    end
 end

  dispatch :mysql_lookup_key do
    param 'Variant[String, Numeric]', :key
    param 'Hash', :options
    param 'Puppet::LookupContext', :context
  end

  def mysql_lookup_key(key, options, context)
    return context.cached_value(key) if context.cache_has_key(key)

    result = mysql_get(key, context, options)

    answer = result.is_a?(Hash) ? result[key] : result
    context.not_found if answer.nil?
    return answer
  end

  def mysql_get(key, context, options)
    host  = options['host']
    user  = options['user']
    pass  = options['pass']
    db    = options['database']
    table = options['table']
    value = options['value_field']
    var   = options['key_field']
    query = "select #{value} from #{table} where #{var}=\"#{key}\""
     
    if $use_jdbc
      Puppet.debug("Using jdbc")
      result = {}
      begin
        Java::com.mysql.jdbc.Driver
        url = "jdbc:mysql://#{host}:3306/#{db}"
        Puppet.debug("Connecting to #{url}")

        conn = java.sql.DriverManager.get_connection(url, user, pass)
        stmt = conn.create_statement

        res = smtm.execute_query(query)
        md = res.getMetaData
        numcols = md.getColumnCount

        Puppet.debug("Mysql returned #{numcols} rows")
        return res
#      rescue Java::Error => e
#        raise Puppet::DataBinding::LookupError, "Mysql connections failed #{e.errno}: #{e.error}"
      ensure 
        conn.close if conn
      end
    else
      Puppet.debug("Not using jdbc")
      begin 
        conn = Mysql.new host, user, pass, db
        Puppet.debug("Hiera-mysql: MySQL connection to #{host} established")
        Puppet.debug("Hiera-mysql: Attempting query #{query}")
        rs = conn.query query
        answer = rs.fetch_row
        if answer.is_a?(Array)
          value = answer[0]
        elsif answer.is_a?(String)
          value = answer
        else
          value = nil
        end

        return value
      rescue Mysql::Error => e
        raise Puppet::DataBinding::LookupError, "Mysql connections failed #{e.errno}: #{e.error}"

      ensure
        conn.close if conn
      end
    end
  end

end


