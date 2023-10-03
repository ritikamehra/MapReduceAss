from mrjob.job import MRJob
from datetime import datetime

# We extend the MRJob class 
# This includes our definition of map and reduce functions
class MyMapReduce(MRJob):


    # The mapper produces a list of (key, value)
    # key is the partition and value is the revenue for each pair.
    def mapper(self, _, line):
        vendor = line.strip()
        vendor = vendor.split(",")
        if (vendor[0] != "VendorID"):        #ignore header        
            t1 = datetime.strptime(vendor[1], "%Y-%m-%d %H:%M:%S") #extract timestamp
            partition = t1.strftime("%b")  + '-' + t1.strftime("%a")  #partition: Month - Day
            yield (partition,float(vendor[16])) #partition and revenue
    
    
    # The reducer produces one instance for each partition and its average revenue.
    def reducer(self, partition, revenue): 
        total_rev = 0
        count = 0

        for rev in revenue:
            total_rev += rev
            count += 1

        yield(partition , total_rev/count)

            
if __name__ == '__main__':
    MyMapReduce.run()

