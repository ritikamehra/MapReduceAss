import happybase
#create connection
connection = happybase.Connection('localhost', port =9090, autoconnect =False)

#open connection to perform operations
def open_connection():
    connection.open()
#close the opened connection
def close_connection():
    connection.close()
#get the pointer to a table
def get_table(name):
    open_connection()
    table = connection.table(name)
    close_connection()
    return table
#batch insert data in trip_details table
def batch_insert_data(filename, lastrow):
    print("starting batch insert of file ", filename)
    file = open(filename, "r")
    table = get_table('trip_details')
    open_connection()
    i = 0
    counter = lastrow
    with table.batch(batch_size=10000) as b:
        for line in file:
            if i!=0:
                temp = line.strip().split(",")
                # this put() will result in two mutations (two cells)
                b.put(counter , { 'td: VendorID' : temp[0] })
                b.put(counter , { 'td: tpep_pickup_datetime' : temp[1] })
                b.put(counter , { 'td: tpep_dropoff_datetime'  : temp[2]})
                b.put(counter , { 'td: passenger_count' : temp[3] })
                b.put(counter , { 'td: trip_distance' : temp[4] })
                b.put(counter , { 'td: RatecodeID'  : temp[5]})
                b.put(counter , { 'td: store_and_fwd_flag' : temp[6]})
                b.put(counter , { 'td: PULocationID' : temp[7] })
                b.put(counter , { 'td: DOLocationID' : temp[8] })
                b.put(counter , { 'td: payment_type' : temp[9] })
                b.put(counter , { 'td: fare_amount' : temp[10] })
                b.put(counter , { 'td: extra' : temp[11]})
                b.put(counter , { 'td: mta_tax' : temp[12]})
                b.put(counter , { 'td: tip_amount' : temp[13]})
                b.put(counter , { 'td: tolls_amount' : temp[14]})
                b.put(counter , { 'td: improvement_surcharge' : temp[15]})
                b.put(counter , { 'td: total_amount' : temp[16]})
                b.put(counter , { 'td: congestion_surcharge' : temp[17]})
                b.put(counter , { 'td: airport_fee' : temp[18]})
                
                counter = str(int(counter)+1)
            i+=1
            
    file.close()
    print("batch insert done for file ", filename)
    close_connection()
    return counter

lastrow = batch_insert_data('yellow_tripdata_2017-03.csv','18880596')
lastrow2 = batch_insert_data('yellow_tripdata_2017-04.csv',lastrow)
