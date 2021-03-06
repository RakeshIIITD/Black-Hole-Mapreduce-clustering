from StarMove import A
from Fitness import B
from mrjob.job import MRJob
import sys
import pickle
from random import randint , uniform
from math import fabs,pow


file_name = '/tmp/dumped_file'	# star file
data_file_name = '/tmp/cache'		# data sum file

def find_min(dictionary,star_keys):
   #print 'Running...'                 # COMMENT THIS TO SPEED UP EXECUTION 
   minimum = sys.maxint     # replace with INT_MAX
   min_starID = -1
   
   for i in dictionary.keys():                       # find min fitness
     if dictionary[i]<minimum:
	min_starID = i
	minimum = dictionary[i]
	
   return min_starID

  
def dump_file(_data , _file):
  f = open(_file, "w")
  pickle.dump(_data, f)
  f.close()

def put_values(runner,job):
    c={}
    it = 0 
    l = runner.stream_output()
    for line in l:
	    it+=1
	    key , value = job.parse_output_line(line)
	    #if it==2:
		#print line
		#print key , value
		#print type(key)
	    c[key] = value
    dump_file(c,file_name)

def put_data_values(runner,job):		# for starID , sum
    c = {}
    it = 0
    l = runner.stream_output()
    for line in l:
	    it+=1
	    key , value = job.parse_output_line(line)
	    #if it==2:
	     # print value
	     # print type(value)
	    c[key] = value
    dump_file(c,data_file_name)
    
def open_data_values():
    f = open(data_file_name)
    c = pickle.load(f)
    f.close()
    return c

def open_values():
    f = open(file_name)
    d = pickle.load(f)
    f.close()
    return d


#/////////////////////         MERGE         /////////////////////////////////////
def Merge():   # total_fit'''
        new_total_fit = 0.0
        dictionary = open_data_values()		# open values obtained from fitness module
        star = {}
        star = open_values()        		# open all stars
        for i in dictionary.keys():
	  star[i][3] = dictionary[i]                   # update star fitnesses obtained from fitness module output
	
        # calculate new total_fit 
        for i in star.keys():			     # calculating new_total_fit
	  new_total_fit+=star[i][3]
	  
        min_starID = find_min(dictionary,star.keys())   # find starID having minimum fitness 
        if star[1][4]>star[min_starID][3]:		# if starID having minimum fitness have star_fit less than current bh_fit
	  new_bh_loc = star[min_starID][1]		# then update star_loc and star_fit of that star in all other stars as bh_fit and bh_loc
	  new_bh_fit = star[min_starID][3]		
	  new_total_fit = new_total_fit+new_bh_fit	# add bh_fit of that star in new_total_fit
	  for i in star.keys():
	    star[i][2] = new_bh_loc			# updating all stars with bh_fit and bh_loc
	    star[i][4] = new_bh_fit
        else:						# if it is not better then keep previous black hole
	  new_total_fit = new_total_fit + star[1][4]	# if there is no better star which have 'better' than black hole
						# then add previous black hole fitness (bh_fit) to new_total_fit
        # now update total fitness in all stars
        for i in star.keys():
	  #print len(star[i])
	  star[i][5] = new_total_fit
      
	print 'Total_fit : ',new_total_fit,' Black Hole Fitness :',star[1][4]
        dump_file(star,file_name)			# finally dump the updated star data in file
    

    
if __name__=='__main__':
  arg = sys.argv[1]
  #//////////////////////////////////RANGE FILE /////////////////////////////////
  
  f1 = open('glass_range.txt','r')
  min_range = []
  max_range = []
  while True:
    line = f1.readline()
    if len(line)<2:
      break
    line = line.split(",")
    min_range.append(float(line[0]))
    max_range.append(float(line[1]))
    
  f1.close()

   
#///////////////// SINGLE STAR INDEX VALUES MEANING   ///////////////////////////
  # star[starID] [0] = starID
  # star[starID] [1] = star_loc
  # star[starID] [2] = bh_loc
  # star[starID] [3] = star_fit
  # star[starID] [4] = bh_fit
  # star[starID] [5] = total_fit
#///////////////////////////////////////////////////////////////////////////////

  #////////////////////   ENTER DATA  ////////////////////////////////////////////////
  
  n_iter = 0 			# No. of iterations
  iterations = 100                 # Total iterations
  K = 3   # number of clusters
  n = 214  # number of datapoints
  dimension = 9
  no_of_stars = 100
  #////////////////////// ENTER RANGE DATA  ///////////////////////////////////////////////
  #min_range = [4.3,2,1,0.1]    # enter (min,max) range of each dimension in this list : for eg , in 3 dimensions , min_range == [ 1 ,2 ,3 ] 
  #max_range = [7.9,4.4,6.9,2.5]	  #   max_range = [9,10,11]
  #min_fit = 0		# for star , black hole minimum fitness range
  #max_fit = 1		# for star , black hole maximum fitness range
  #//////////////////////CLUSTER CENTROID FILE////////////////////////////////////
  f2 = open('s','w')
  
  for i in range(1,1 + no_of_stars ):
    f2.write('%d\n'%i)
  f2.close()

  #/////////////////////////   RANDOM    /////////////////////////////////////////
  star = {}
  bh_loc = []      #bh_loc
  
  for i in range(0,K):
    bh_loc.append([uniform(min_range[i] , max_range[i]) for i in range(0,dimension)])
  bh_fit = sys.maxint
  total_fit = bh_fit + 0.0
  
  for j in range(1 , no_of_stars + 1 ):     # generating  stars
    single_star = []
    single_star.append(j)
    single_star_loc = []
    
    for i in range(0,K):
	single_star_loc.append([uniform(min_range[i] , max_range[i]) for i in range(0,dimension)])
	
    single_star.append(single_star_loc)
    single_star.append(bh_loc)
    star_fit = uniform(0,10)
    single_star.append(star_fit)
    total_fit+= star_fit
    single_star.append(bh_fit)
    star[j]=single_star
  
  for i in range(1 , no_of_stars + 1):
    star[i].append(total_fit)
     
  dump_file(star,file_name)

  #////////////////////////////////////////ITERATONS/////////////////////////////////
  min_range = str(min_range).strip("[").strip("]")
  max_range = str(max_range).strip("[").strip("]")
  while True:
    
    n_iter+=1
    print 'iteration : ',n_iter                  # COMMENT THIS TO SPEED UP EXECUTION 
    if iterations==n_iter:
      break
    
    
    job = A(args = ['-r','local',sys.argv[2]]+['--n_iter='+str(n_iter)]+['--no_of_stars='+str(no_of_stars) ,'--min_range='+str(min_range),'--max_range='+str(max_range)]+['--dimension='+str(dimension)]+['--n='+str(n)]+['--K='+str(K)]+['--star='+file_name])
    with job.make_runner() as runner:
      runner.run()
      put_values(runner,job)
    
    job = B(args = ['-r','local',sys.argv[1]]+['--n_iter='+str(n_iter)]+['--star='+file_name])
    with  job.make_runner() as runner:
      runner.run()
      put_data_values(runner,job)   # correct : no  need to  dump file , collect direct stream to merge
    
    Merge()
    
#////////   PRINT OUTPUT ////////////////////////////////////////////////////////////

  k = open_values()
  print k[1][5]                  # total_fit