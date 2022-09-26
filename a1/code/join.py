import MapReduce
import sys

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

#Mappe Input: [table_identifier, join_key, sequence of other columns]
def mapper(record):
   #record: json 파일 한줄씩 읽어옴
   join_key = record[1]
   for item in record:
    mr.emit_intermediate(join_key, item)


#[sequence of the columns of ORDER, sequence of the columns of LINEITEM]
def reducer(key, list_of_values): 
    mr.emit(list_of_values)
      


# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
