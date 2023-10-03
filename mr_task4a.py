from mrjob.job import MRJob
from mrjob.step import MRStep
# We extend the MRJob class 
# This includes our definition of map and reduce functions
class MyMapReduce(MRJob):
    
    def steps(self):
        return[
            MRStep (mapper=self.mapper,
            reducer=self.reducer),
            MRStep(reducer=self.reducer_max)
        ]

    # The mapper produces a list of (key, value)
    # key is the vendor id and value is the revenue for each pair.
    def mapper(self, _, line):
        vendor = line.strip()
        vendor = vendor.split(",")
        if (vendor[0] != "VendorID"): #ignore header
            yield (vendor[0], float(vendor[16])) #vendor id and revenue
    
    #reducer will yield one instance for each vendorid, with count and total revenue
    def reducer(self, vendorid, revenue):
        coutn = 0
        totrev = 0 
        for rev in revenue:
            totrev += rev
            count += 1
        yield None,(count,(vendorid,totrev))

    #reducer_max will yield the vendorid with max count and its total revenue
    def reducer_max(self, _, pair):                
        yield(max(pair)[1])


if __name__ == '__main__':    
    MyMapReduce.run()

