from monary import Monary
m = Monary()
pipeline = [{"$group" : {"_id" : "$state", "totPop" : { "$sum" : "$pop"}}}]
states, population = m.aggregate("zips","data", pipeline, ["_id","totpop"], ["string:2", "int64"])
strs = list(map(lambda x: x.decode("utf-8"), states))
result = list("%s: %d" % (state, pop)
     for (state, pop) in zip(strs, population))