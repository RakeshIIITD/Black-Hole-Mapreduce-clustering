from mrjob.job import MRJob
from random import randint , uniform
from math import fabs,pow,sqrt
import pickle
import sys

#///////////////////////// FITNESS ///////////////////////////////////////

class B(MRJob):
  
    def __init__(self, args):
	MRJob.__init__(self, args)
	
    def read_centroids(self):				# READ_CENTROIDS
	f0 = open(self.options.star)
	f1 = open(self.options.star)
	self.matrix = pickle.load(f1)
	self.star = {}
	self.star = pickle.load(f0)
	f0.close()
	f1.close()
	
    def steps(self):						# STEPS
	
	return [self.mr(mapper_init = self.read_centroids,
                       mapper = self.Fitness_Mapper,
                       reducer = self.Fitness_Reducer)]
                
    def configure_options(self):				# CONFIGURE OPTIONS
	super(B, self).configure_options()
	self.add_passthrough_option('--n_iter', type='int', help='Number of iter')
	#self.add_passthrough_option('--K' , type ='int' , help='Number of clusters')
	#self.add_passthrough_option('--n' , type = 'int' , help= "Number of samples")
	#self.add_passthrough_option('--dimensions' , type = 'int' , help= "Number of dimensions")
	self.add_file_option('--star')
	
    def dist(self,star_loc,bh_loc):
	return sqrt(sum([fabs(float(star_loc[i])-float(bh_loc[i])) for i in range(0,len(star_loc))]))
    
    def Fitness_Mapper(self, _ , data ):
	data = data.split(",")
	#dataID = int(data[0])
	data = [float(data[i]) for i in range(0,len(data))]
        starList = self.star
        K = len(self.star[1][1])
        mindistance = sys.maxint
        min_starID = -1
        for star in starList.keys():
	    for i in range(0,K):
		distance = self.dist( data , starList[star][1][i]) 
		if distance<mindistance:
		   mindistance = distance
		   min_starID = star
	    yield star , mindistance

    def Fitness_Reducer(self,starID , mindistance_list):
        Sum = 0.0
        for mindistance in mindistance_list:
            Sum = Sum + mindistance
        yield starID , Sum
        
    #def Fitness_Reducer_final(self):			# Save star Data
     #   f = open(self.options.star,"w")
      #  pickle.dump( self.star , f )
       # f.close()
        
	
        
if __name__=='__main__':
  B.run()