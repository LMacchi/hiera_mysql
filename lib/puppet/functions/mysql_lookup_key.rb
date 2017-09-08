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

    result = mysql_get(key, context, options)

    answer = result.is_a?(Hash) ? result[key] : result
    context.not_found if answer.nil?
    return answer
  end

  def mysql_get(key, context, options)
    begin 
      con = Mysql.new options['host'], options['user'], options['pass'], options['database']
      Puppet.debug("Hiera-mysql: MySQL connection to #{options['host']} established")
      table = options['table']
      query = "select val from #{table} where var=\"#{key}\""
      Puppet.debug("Hiera-mysql: Attempting query #{query}")
      rs = con.query query
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
      con.close if con
    end
  end

end


