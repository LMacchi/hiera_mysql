Puppet::Functions.create_function(:mysql_lookup_key) do

  begin
    require 'mysql'
  rescue LoadError => e
    raise Puppet::DataBinding::LookupError, "Must install mysql gem to use hiera-mysql"
  end

  dispatch :mysql_lookup_key do
    param 'Variant[String, Numeric]', :key
    param 'Hash', :options
    param 'Puppet::LookupContext', :context
  end

  def mysql_lookup_key(key, options, context)
    return context.cached_value(key) if context.cache_has_key(key)

    unless options.include?('pass')
      raise ArgumentError, "'mysql_lookup_key': 'pass' must be declared in hiera.yaml when using this lookup_key function"
    end

    result = mysql_get(key, context, options)

    answer = result.is_a?(Hash) ? result[key] : result
    context.not_found if answer.nil?
    return answer
  end

  def mysql_get(key, context, options)
    begin 
      host  = options['host']        || 'localhost'
      user  = options['user']        || 'hiera'
      db    = options['database']    || 'hiera'
      table = options['table']       || 'hiera'
      value = options['value_field'] || 'value'
      var   = options['key_field']   || 'key'
      pass  = options['pass']
      query = "select #{value} from #{table} where #{var}=\"#{key}\""

      conn = Mysql.new host, user, pass, db
      Puppet.debug("Hiera-mysql: MySQL connection to #{options['host']} established")
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
