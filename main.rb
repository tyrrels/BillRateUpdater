class RateRecord
  
  attr_accessor :table_no
  attr_accessor :employee_no
  attr_accessor :rate
  attr_accessor :start_date
  attr_accessor :end_date
  
  def initialize(table_no,employee_no,rate,start_date)
    @table_no = table_no
    @employee_no = employee_no
    @rate = rate
    @start_date = start_date
  end
  
  def key
    @employee_no + @start_date
  end
  
  def list
    @table_no+','+@employee_no+','+@rate+','+"#{@employee_no}#{@start_date}"+','+@start_date
  end
  
  def csv_output
    csv = "INSERT INTO BTRRTEmpls (TableNo, Employee, Rate, RateID, EffectiveDate, StartDate, EndDate) Select "
    csv << @table_no << ",\'" 
    csv << @employee_no << "\',"
    csv << @rate << ",\'"
    csv << "#{@employee_no}#{@start_date}" << "\',\'"
    csv << @start_date << "\',\'"
    csv << @start_date << "\',\'9999-01-01 00:00:00.000\'"
    
    csv
  end
end

def process_table(data)
  in_data = data.sort_by{|x| [x.employee_no, x.start_date]}.collect.group_by{|x| x.employee_no}  
  new_data = []
  
  in_data.each {|data| new_data.push(process_data(data[1]))}
  
  new_data
end

def process_data(data)
  new_data = []
    
  new_data << data.shift
  last_rate = new_data[0].rate
  data.each do |a|
    if (a.rate != last_rate)
      new_data << a
      last_rate = a.rate
    end
  end 
  
  new_data
end

unless ARGV.length == 1
  puts "Input file expected"
  exit
end

input_file = ARGV[0]

puts "Reading from #{input_file}"

input_data = []

File.open(input_file, "r") do |f|

  f.each_line {|line|
    if (line.empty?)
    else
      words = line.split(',')
      record = RateRecord.new(words[0],words[1],words[2],words[4])    
      record.start_date = record.start_date[0,10]    
      input_data.push(record)
    end
  }

end
puts "Data in #{input_data.length}"
print "Processing data... "
input_data = input_data.group_by{|x| x.table_no}

new_data = []

input_data.each do |table|
  new_data.push(process_table(table[1]))
end

puts "Done."
new_data.flatten!

File.open("data_new.csv","w") do |outFile|
  new_data.each {|item| outFile.puts(item.csv_output)}
end

puts "Data out #{new_data.length}"
