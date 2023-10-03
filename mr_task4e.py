from mrjob.job import MRJob
from mrjob.step import MRStep
# We extend the MRJob class
# This includes our definition of map and reduce functions
class MyMapReduce(MRJob):

    
    def steps(self):
        return[
            MRStep (mapper=self.mapper,
            reducer=self.reducer),
            MRStep (reducer=self.sort_by_avg)
        ]
        
    # The mapper produces a list of (key, value)
    # key is the pickup location id and value is the tip to revenue ratio.
    def mapper(self, _, line):
        vendor = line.strip()
        vendor = vendor.split(",")
        if (vendor[0] != "VendorID"):      #ignoring header
            if float(vendor[13]) != 0:     #to handle division error
                tip_rev = float(vendor[13])/float(vendor[16]) #tip to revenue ratio
            else: 
                tip_rev = 0
            yield (vendor[7],tip_rev) #pickup location id and tip to revenue ratio

     # The reducer produces one instance for each pick up location and its average tip to revenue ratio.
    def reducer(self, pulocid, tip_rev):
        total_tiprev = 0
        count = 0

        for rev in tip_rev:
            total_tiprev += rev
            count += 1

        yield(None, (total_tiprev/count, pulocid))

# The sort_by_avg produces one instance for each pickup location id and its tip to revenue ratio in sorted order.
    def sort_by_avg(self, _, pair):
        sorted_pairs = sorted(pair, reverse=True)
        for pair in sorted_pairs:
            yield pair


if __name__ == '__main__':
    MyMapReduce.run()
