from mrjob.job import MRJob
from datetime import datetime

# We extend the MRJob class 
# This includes our definition of map and reduce functions
class MyMapReduce(MRJob):
    
    # The mapper produces a list of (key, value)
    # key is the pickup location id and value is the triptime in minutes.
    def mapper(self, _, line):
        vendor = line.strip()
        vendor = vendor.split(",")
        if (vendor[0] != "VendorID"): #ignoring header
            t1 = datetime.strptime(vendor[1], "%Y-%m-%d %H:%M:%S") #pickup time
            t2 = datetime.strptime(vendor[2], "%Y-%m-%d %H:%M:%S") #drop off time
            triptime = (t2-t1).total_seconds() #converting time to seconds
            yield (vendor[7], float(triptime/60)) #converting time to minutes
    
    # The reducer produces one instance for each pick up location and its average triptime.
    def reducer(self, pulocid, triptime): 
        total_time = 0
        count = 0
        for time in triptime:
            total_time += time
            count += 1
    
        yield (pulocid, total_time/count)

if __name__ == '__main__':
    MyMapReduce.run()