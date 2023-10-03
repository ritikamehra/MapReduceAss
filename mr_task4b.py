from mrjob.job import MRJob
from mrjob.step import MRStep

# We extend the MRJob class 
# This includes our definition of map and reduce functions
class MyMapReduce(MRJob):

    
    def steps(self):
        return[
            MRStep(mapper=self.mapper,
            reducer=self.reducer),
            MRStep(reducer=self.reducer_max)
        ]

    # The mapper produces a list of (key, value)
    # key is the pickup location id and value is the revenue.
    def mapper(self, _, line):
        vendor = line.strip()
        vendor = vendor.split(",")
        if (vendor[0] != "VendorID"):         #ignore header   
            yield (vendor[7], float(vendor[16])) #pickup location id and revenue
    
    # The reducer produces one instance for each pickup location and its revenue.
    def reducer(self, pulocid, revenue):
        yield None, (sum(revenue), pulocid)

    # The reducer_max produces pickup location that has the highest revenue.
        
    def reducer_max(self, _, loc_rev_pairs):
        yield(max(loc_rev_pairs))

if __name__ == '__main__':
    MyMapReduce.run()

