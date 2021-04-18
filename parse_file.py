#////////////////////  PARSE FILE   /////////////////////
# -- for parsing file before mapreduce processing --
input_file = 'iris'
outfile = 'iris2'



if __name__=='__main__':
  f1 = open(input_file,"r")
  f2 = open(outfile,"w")
  counter = 1
  while True:
    line = f1.readline()
    if len(line)==0:
      break
    line = str(counter)+','+line
    counter = counter + 1
    f2.write('%s\n'%(line))
  f1.close()
  f2.close()