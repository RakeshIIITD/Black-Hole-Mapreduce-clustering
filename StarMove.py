from mrjob.job import MRJob
from random import randint , uniform
from math import fabs,pow,sqrt
import pickle
import sys
import logging
#distance changed using sqrt
log = logging.getLogger('mrjob.local')

class A(MRJob):
    
    def __init__(self, args):
	MRJob.__init__(self, args)
	
    def read_centroids(self):				# READ_CENTROIDS
	f = open(self.options.star)
	self.star = {}
	self.star = pickle.load(f)
	f.close()
	self.options.min_range = self.options.min_range.split(",")
	self.options.max_range = self.options.max_range.split(",")
	
    def steps(self):						# STEPS
	
	return [self.mr(mapper_init = self.read_centroids,
                        mapper = self.StarMove_Mapper,
                        reducer = self.StarMove_Reducer)]
                
    def configure_options(self):				# CONFIGURE OPTIONS
	super( A , self).configure_options()
	self.add_passthrough_option('--n_iter', type='int', help='Number of iter')
	self.add_passthrough_option('--no_of_stars' , type = 'int' , help = 'Number of stars')
	self.add_passthrough_option('--min_fit' , type = 'int' , help= "Number of dimensions")
	self.add_passthrough_option('--max_fit' , type = 'int' , help= "Number of dimensions")
	self.add_passthrough_option('--min_range' , type = 'str' , help= "Number of dimensions")
	self.add_passthrough_option('--max_range' , type = 'str' , help= "Number of dimensions")
	self.add_passthrough_option('--dimension' , type = 'int' , help= "Number of dimensions")
	self.add_passthrough_option('--n' , type = 'int' , help= "Number of samples")
	self.add_passthrough_option('--K' , type ='int' , help='Number of clusters')
	self.add_file_option('--star')
		
    #def dist(self,star_loc,bh_loc):
     #   return sum([pow(fabs(float(star_loc[i])-float(bh_loc[i])),2) for i in range(0,len(star_loc))])

    def event_horizon(self, bh_fit , total_fit):
        return float(bh_fit)/total_fit
    
    def random_star(self , starID , bh_loc , bh_fit , total_fit ):
        l = []
        star_loc = []
        
        for i in range(0,self.options.K):
	  star_loc.append([uniform(float(self.options.min_range[i]) , float(self.options.max_range[i])) for i in range(0,self.options.dimension)])
	
	l.append(starID)
	l.append(star_loc)
	l.append(bh_loc)
	l.append(uniform(0,6))	# star_fit
	l.append(bh_fit)		# bh_fit
	l.append(total_fit)
	
        self.star[starID] = l  # update
    
    def StarMove_Mapper(self,_,value):
        
        starID = int(value)   # using data id for starID
        #self.data[starID] = [float(value[i]) for i in range(1,len(value))]   # data
        star = self.star[starID]
        rand = uniform(0,1)   # fuzzy random number
        # Moving stars
        for i in range(0,self.options.K):
            if self.options.n_iter==1:
                yield starID , star
                break
            else:
		for j in range(0,self.options.dimension):
		    star[1][i][j] = star[1][i][j] + rand*(star[2][i][j] - star[1][i][j] )    # correction here
                    
        #checking event horizon
        sys.stderr.write("MAPPER : {0}\n".format(star))
        star_loc = star[1]
        
        bh_loc = star[2]
        bh_fit = star[4]
        total_fit = star[5]
	#except IndexError:
	#  pass
	  #print IndexError , star
	sum_distance = 0.0
	
	for i in range(0,self.options.K):
	  for j in range(0,self.options.dimension):
	    sum_distance = sum_distance + pow(fabs(float(star_loc[i][j])-float(bh_loc[i][j])),2)
	    
        distance  = sqrt(sum_distance)  # passing dist(star_loc , bh_loc)
        
        radius = self.event_horizon(star[4] , star[5]) # passing radius(bh_fit , total_fit)
        if distance<radius:                         # if distance is less than radius of event horizon
            self.random_star(starID,bh_loc,bh_fit,total_fit)     # creating random star and updating
            
        yield starID , self.star[starID]     

    def StarMove_Reducer(self,starID,star_list):
	for star  in star_list:
	    yield starID , star

if __name__=='__main__':
  A.run()