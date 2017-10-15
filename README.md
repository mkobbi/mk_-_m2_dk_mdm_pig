#-----------------------------------------------
# New Logical Plan:
#-----------------------------------------------
filtered_by_count: (Name: LOStore Schema: userId#22:int,count#52:long)
|
|---filtered_by_count: (Name: LOFilter Schema: userId#22:int,count#52:long)
    |   |
    |   (Name: GreaterThanEqual Type: boolean Uid: 55)
    |   |
    |   |---count:(Name: Project Type: long Uid: 52 Input: 0 Column: 1)
    |   |
    |   |---(Name: Constant Type: long Uid: 53)
    |
    |---count_by_user: (Name: LOForEach Schema: userId#22:int,count#52:long)
        |   |
        |   (Name: LOGenerate[false,false] Schema: userId#22:int,count#52:long)ColumnPrune:OutputUids=[52, 22]ColumnPrune:InputUids=[48, 22]
        |   |   |
        |   |   group:(Name: Project Type: int Uid: 22 Input: 0 Column: (*))
        |   |   |
        |   |   (Name: UserFunc(org.apache.pig.builtin.COUNT_STAR) Type: long Uid: 52)
        |   |   |
        |   |   |---(Name: Dereference Type: bag Uid: 51 Column:[0])
        |   |       |
        |   |       |---A:(Name: Project Type: bag Uid: 48 Input: 1 Column: (*))
        |   |
        |   |---(Name: LOInnerLoad[0] Schema: group#22:int)
        |   |
        |   |---A: (Name: LOInnerLoad[1] Schema: userId#22:int,movieId#23:int,rating#24:float,timestamp#25:long)
        |
        |---grouped_by_user: (Name: LOCogroup Schema: group#22:int,A#48:bag{#60:tuple(userId#22:int,movieId#23:int,rating#24:float,timestamp#25:long)})
            |   |
            |   userId:(Name: Project Type: int Uid: 22 Input: 0 Column: 0)
            |
            |---A: (Name: LOForEach Schema: userId#22:int,movieId#23:int,rating#24:float,timestamp#25:long)
                |   |
                |   (Name: LOGenerate[false,false,false,false] Schema: userId#22:int,movieId#23:int,rating#24:float,timestamp#25:long)ColumnPrune:OutputUids=[22, 23, 24, 25]ColumnPrune:InputUids=[22, 23, 24, 25]
                |   |   |
                |   |   (Name: Cast Type: int Uid: 22)
                |   |   |
                |   |   |---userId:(Name: Project Type: bytearray Uid: 22 Input: 0 Column: (*))
                |   |   |
                |   |   (Name: Cast Type: int Uid: 23)
                |   |   |
                |   |   |---movieId:(Name: Project Type: bytearray Uid: 23 Input: 1 Column: (*))
                |   |   |
                |   |   (Name: Cast Type: float Uid: 24)
                |   |   |
                |   |   |---rating:(Name: Project Type: bytearray Uid: 24 Input: 2 Column: (*))
                |   |   |
                |   |   (Name: Cast Type: long Uid: 25)
                |   |   |
                |   |   |---timestamp:(Name: Project Type: bytearray Uid: 25 Input: 3 Column: (*))
                |   |
                |   |---(Name: LOInnerLoad[0] Schema: userId#22:bytearray)
                |   |
                |   |---(Name: LOInnerLoad[1] Schema: movieId#23:bytearray)
                |   |
                |   |---(Name: LOInnerLoad[2] Schema: rating#24:bytearray)
                |   |
                |   |---(Name: LOInnerLoad[3] Schema: timestamp#25:bytearray)
                |
                |---A: (Name: LOLoad Schema: userId#22:bytearray,movieId#23:bytearray,rating#24:bytearray,timestamp#25:bytearray)RequiredFields:null
#-----------------------------------------------
# Physical Plan:
#-----------------------------------------------
filtered_by_count: Store(fakefile:org.apache.pig.builtin.PigStorage) - scope-31
|
|---filtered_by_count: Filter[bag] - scope-27
    |   |
    |   Greater Than or Equal[boolean] - scope-30
    |   |
    |   |---Project[long][1] - scope-28
    |   |
    |   |---Constant(100) - scope-29
    |
    |---count_by_user: New For Each(false,false)[bag] - scope-26
        |   |
        |   Project[int][0] - scope-20
        |   |
        |   POUserFunc(org.apache.pig.builtin.COUNT_STAR)[long] - scope-24
        |   |
        |   |---Project[bag][0] - scope-23
        |       |
        |       |---Project[bag][1] - scope-22
        |
        |---grouped_by_user: Package(Packager)[tuple]{int} - scope-17
            |
            |---grouped_by_user: Global Rearrange[tuple] - scope-16
                |
                |---grouped_by_user: Local Rearrange[tuple]{int}(false) - scope-18
                    |   |
                    |   Project[int][0] - scope-19
                    |
                    |---A: New For Each(false,false,false,false)[bag] - scope-15
                        |   |
                        |   Cast[int] - scope-4
                        |   |
                        |   |---Project[bytearray][0] - scope-3
                        |   |
                        |   Cast[int] - scope-7
                        |   |
                        |   |---Project[bytearray][1] - scope-6
                        |   |
                        |   Cast[float] - scope-10
                        |   |
                        |   |---Project[bytearray][2] - scope-9
                        |   |
                        |   Cast[long] - scope-13
                        |   |
                        |   |---Project[bytearray][3] - scope-12
                        |
                        |---A: Load(/home/mkobbi/workspace/loana_lab2_mkobbi/ml-20m/ratings.csv:PigStorage(',')) - scope-2

#--------------------------------------------------
# Map Reduce Plan                                  
#--------------------------------------------------
MapReduce node scope-32
Map Plan
grouped_by_user: Local Rearrange[tuple]{int}(false) - scope-45
|   |
|   Project[int][0] - scope-47
|
|---count_by_user: New For Each(false,false)[bag] - scope-33
    |   |
    |   Project[int][0] - scope-34
    |   |
    |   POUserFunc(org.apache.pig.builtin.COUNT_STAR$Initial)[tuple] - scope-35
    |   |
    |   |---Project[bag][0] - scope-36
    |       |
    |       |---Project[bag][1] - scope-37
    |
    |---Pre Combiner Local Rearrange[tuple]{Unknown} - scope-48
        |
        |---A: New For Each(false,false,false,false)[bag] - scope-15
            |   |
            |   Cast[int] - scope-4
            |   |
            |   |---Project[bytearray][0] - scope-3
            |   |
            |   Cast[int] - scope-7
            |   |
            |   |---Project[bytearray][1] - scope-6
            |   |
            |   Cast[float] - scope-10
            |   |
            |   |---Project[bytearray][2] - scope-9
            |   |
            |   Cast[long] - scope-13
            |   |
            |   |---Project[bytearray][3] - scope-12
            |
            |---A: Load(/home/mkobbi/workspace/loana_lab2_mkobbi/ml-20m/ratings.csv:PigStorage(',')) - scope-2--------
Combine Plan
grouped_by_user: Local Rearrange[tuple]{int}(false) - scope-49
|   |
|   Project[int][0] - scope-51
|
|---count_by_user: New For Each(false,false)[bag] - scope-38
    |   |
    |   Project[int][0] - scope-39
    |   |
    |   POUserFunc(org.apache.pig.builtin.COUNT_STAR$Intermediate)[tuple] - scope-40
    |   |
    |   |---Project[bag][1] - scope-41
    |
    |---grouped_by_user: Package(CombinerPackager)[tuple]{int} - scope-44--------
Reduce Plan
filtered_by_count: Store(fakefile:org.apache.pig.builtin.PigStorage) - scope-31
|
|---filtered_by_count: Filter[bag] - scope-27
    |   |
    |   Greater Than or Equal[boolean] - scope-30
    |   |
    |   |---Project[long][1] - scope-28
    |   |
    |   |---Constant(100) - scope-29
    |
    |---count_by_user: New For Each(false,false)[bag] - scope-26
        |   |
        |   Project[int][0] - scope-20
        |   |
        |   POUserFunc(org.apache.pig.builtin.COUNT_STAR$Final)[long] - scope-24
        |   |
        |   |---Project[bag][1] - scope-42
        |
        |---grouped_by_user: Package(CombinerPackager)[tuple]{int} - scope-17--------
Global sort: false
----------------

(61,4,3.0,1197068409)
--------------------------------------------------------------------------------
| A     | userId:int    | movieId:int    | rating:float    | timestamp:long    | 
--------------------------------------------------------------------------------
|       | 61            | 4              | 3.0             | 1197068409        | 
|       | 31            | 8622           | 0.5             | 1424735179        | 
|       | 41            | 4              | 2.0             | 951694198         | 
|       | 91            | 1172           | 5.0             | 1121672237        | 
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
| B     | userId:int    | movieId:int    | rating:float    | timestamp:long    | 
--------------------------------------------------------------------------------
|       | 61            | 4              | 3.0             | 1197068409        | 
|       | 41            | 4              | 2.0             | 951694198         | 
--------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------
| grouped_by_movie     | group:int    | B:bag{:tuple(userId:int,movieId:int,rating:float,timestamp:long)}                     | 
-------------------------------------------------------------------------------------------------------------------------------
|                      | 4            | {(61, ..., 1197068409), (41, ..., 951694198)}                                         | 
-------------------------------------------------------------------------------------------------------------------------------

#-----------------------------------------------
# New Logical Plan:
#-----------------------------------------------
count_by_movie: (Name: LOStore Schema: movieId#154:int,count#196:long)
|
|---count_by_movie: (Name: LOForEach Schema: movieId#154:int,count#196:long)
    |   |
    |   (Name: LOGenerate[false,false] Schema: movieId#154:int,count#196:long)ColumnPrune:OutputUids=[196, 154]ColumnPrune:InputUids=[192, 154]
    |   |   |
    |   |   group:(Name: Project Type: int Uid: 154 Input: 0 Column: (*))
    |   |   |
    |   |   (Name: UserFunc(org.apache.pig.builtin.COUNT_STAR) Type: long Uid: 196)
    |   |   |
    |   |   |---(Name: Dereference Type: bag Uid: 195 Column:[1])
    |   |       |
    |   |       |---B:(Name: Project Type: bag Uid: 192 Input: 1 Column: (*))
    |   |
    |   |---(Name: LOInnerLoad[0] Schema: group#154:int)
    |   |
    |   |---B: (Name: LOInnerLoad[1] Schema: userId#153:int,movieId#154:int,rating#155:float,timestamp#156:long)
    |
    |---grouped_by_movie: (Name: LOCogroup Schema: group#154:int,B#192:bag{#206:tuple(userId#153:int,movieId#154:int,rating#155:float,timestamp#156:long)})
        |   |
        |   movieId:(Name: Project Type: int Uid: 154 Input: 0 Column: 1)
        |
        |---B: (Name: LOFilter Schema: userId#153:int,movieId#154:int,rating#155:float,timestamp#156:long)
            |   |
            |   (Name: And Type: boolean Uid: 205)
            |   |
            |   |---(Name: GreaterThan Type: boolean Uid: 187)
            |   |   |
            |   |   |---(Name: Cast Type: double Uid: 155)
            |   |   |   |
            |   |   |   |---rating:(Name: Project Type: float Uid: 155 Input: 0 Column: 2)
            |   |   |
            |   |   |---(Name: Constant Type: double Uid: 186)
            |   |
            |   |---(Name: LessThan Type: boolean Uid: 190)
            |       |
            |       |---(Name: Cast Type: double Uid: 155)
            |       |   |
            |       |   |---rating:(Name: Project Type: float Uid: 155 Input: 0 Column: 2)
            |       |
            |       |---(Name: Constant Type: double Uid: 189)
            |
            |---A: (Name: LOForEach Schema: userId#153:int,movieId#154:int,rating#155:float,timestamp#156:long)
                |   |
                |   (Name: LOGenerate[false,false,false,false] Schema: userId#153:int,movieId#154:int,rating#155:float,timestamp#156:long)ColumnPrune:OutputUids=[153, 154, 155, 156]ColumnPrune:InputUids=[153, 154, 155, 156]
                |   |   |
                |   |   (Name: Cast Type: int Uid: 153)
                |   |   |
                |   |   |---userId:(Name: Project Type: bytearray Uid: 153 Input: 0 Column: (*))
                |   |   |
                |   |   (Name: Cast Type: int Uid: 154)
                |   |   |
                |   |   |---movieId:(Name: Project Type: bytearray Uid: 154 Input: 1 Column: (*))
                |   |   |
                |   |   (Name: Cast Type: float Uid: 155)
                |   |   |
                |   |   |---rating:(Name: Project Type: bytearray Uid: 155 Input: 2 Column: (*))
                |   |   |
                |   |   (Name: Cast Type: long Uid: 156)
                |   |   |
                |   |   |---timestamp:(Name: Project Type: bytearray Uid: 156 Input: 3 Column: (*))
                |   |
                |   |---(Name: LOInnerLoad[0] Schema: userId#153:bytearray)
                |   |
                |   |---(Name: LOInnerLoad[1] Schema: movieId#154:bytearray)
                |   |
                |   |---(Name: LOInnerLoad[2] Schema: rating#155:bytearray)
                |   |
                |   |---(Name: LOInnerLoad[3] Schema: timestamp#156:bytearray)
                |
                |---A: (Name: LOLoad Schema: userId#153:bytearray,movieId#154:bytearray,rating#155:bytearray,timestamp#156:bytearray)RequiredFields:null
#-----------------------------------------------
# Physical Plan:
#-----------------------------------------------
count_by_movie: Store(fakefile:org.apache.pig.builtin.PigStorage) - scope-140
|
|---count_by_movie: New For Each(false,false)[bag] - scope-139
    |   |
    |   Project[int][0] - scope-133
    |   |
    |   POUserFunc(org.apache.pig.builtin.COUNT_STAR)[long] - scope-137
    |   |
    |   |---Project[bag][1] - scope-136
    |       |
    |       |---Project[bag][1] - scope-135
    |
    |---grouped_by_movie: Package(Packager)[tuple]{int} - scope-130
        |
        |---grouped_by_movie: Global Rearrange[tuple] - scope-129
            |
            |---grouped_by_movie: Local Rearrange[tuple]{int}(false) - scope-131
                |   |
                |   Project[int][1] - scope-132
                |
                |---B: Filter[bag] - scope-119
                    |   |
                    |   And[boolean] - scope-128
                    |   |
                    |   |---Greater Than[boolean] - scope-123
                    |   |   |
                    |   |   |---Cast[double] - scope-121
                    |   |   |   |
                    |   |   |   |---Project[float][2] - scope-120
                    |   |   |
                    |   |   |---Constant(0.5) - scope-122
                    |   |
                    |   |---Less Than[boolean] - scope-127
                    |       |
                    |       |---Cast[double] - scope-125
                    |       |   |
                    |       |   |---Project[float][2] - scope-124
                    |       |
                    |       |---Constant(5.0) - scope-126
                    |
                    |---A: New For Each(false,false,false,false)[bag] - scope-118
                        |   |
                        |   Cast[int] - scope-107
                        |   |
                        |   |---Project[bytearray][0] - scope-106
                        |   |
                        |   Cast[int] - scope-110
                        |   |
                        |   |---Project[bytearray][1] - scope-109
                        |   |
                        |   Cast[float] - scope-113
                        |   |
                        |   |---Project[bytearray][2] - scope-112
                        |   |
                        |   Cast[long] - scope-116
                        |   |
                        |   |---Project[bytearray][3] - scope-115
                        |
                        |---A: Load(/home/mkobbi/workspace/loana_lab2_mkobbi/ml-20m/ratings.csv:PigStorage(',')) - scope-105

#--------------------------------------------------
# Map Reduce Plan                                  
#--------------------------------------------------
MapReduce node scope-141
Map Plan
grouped_by_movie: Local Rearrange[tuple]{int}(false) - scope-154
|   |
|   Project[int][0] - scope-156
|
|---count_by_movie: New For Each(false,false)[bag] - scope-142
    |   |
    |   Project[int][0] - scope-143
    |   |
    |   POUserFunc(org.apache.pig.builtin.COUNT_STAR$Initial)[tuple] - scope-144
    |   |
    |   |---Project[bag][1] - scope-145
    |       |
    |       |---Project[bag][1] - scope-146
    |
    |---Pre Combiner Local Rearrange[tuple]{Unknown} - scope-157
        |
        |---B: Filter[bag] - scope-119
            |   |
            |   And[boolean] - scope-128
            |   |
            |   |---Greater Than[boolean] - scope-123
            |   |   |
            |   |   |---Cast[double] - scope-121
            |   |   |   |
            |   |   |   |---Project[float][2] - scope-120
            |   |   |
            |   |   |---Constant(0.5) - scope-122
            |   |
            |   |---Less Than[boolean] - scope-127
            |       |
            |       |---Cast[double] - scope-125
            |       |   |
            |       |   |---Project[float][2] - scope-124
            |       |
            |       |---Constant(5.0) - scope-126
            |
            |---A: New For Each(false,false,false,false)[bag] - scope-118
                |   |
                |   Cast[int] - scope-107
                |   |
                |   |---Project[bytearray][0] - scope-106
                |   |
                |   Cast[int] - scope-110
                |   |
                |   |---Project[bytearray][1] - scope-109
                |   |
                |   Cast[float] - scope-113
                |   |
                |   |---Project[bytearray][2] - scope-112
                |   |
                |   Cast[long] - scope-116
                |   |
                |   |---Project[bytearray][3] - scope-115
                |
                |---A: Load(/home/mkobbi/workspace/loana_lab2_mkobbi/ml-20m/ratings.csv:PigStorage(',')) - scope-105--------
Combine Plan
grouped_by_movie: Local Rearrange[tuple]{int}(false) - scope-158
|   |
|   Project[int][0] - scope-160
|
|---count_by_movie: New For Each(false,false)[bag] - scope-147
    |   |
    |   Project[int][0] - scope-148
    |   |
    |   POUserFunc(org.apache.pig.builtin.COUNT_STAR$Intermediate)[tuple] - scope-149
    |   |
    |   |---Project[bag][1] - scope-150
    |
    |---grouped_by_movie: Package(CombinerPackager)[tuple]{int} - scope-153--------
Reduce Plan
count_by_movie: Store(fakefile:org.apache.pig.builtin.PigStorage) - scope-140
|
|---count_by_movie: New For Each(false,false)[bag] - scope-139
    |   |
    |   Project[int][0] - scope-133
    |   |
    |   POUserFunc(org.apache.pig.builtin.COUNT_STAR$Final)[long] - scope-137
    |   |
    |   |---Project[bag][1] - scope-151
    |
    |---grouped_by_movie: Package(CombinerPackager)[tuple]{int} - scope-130--------
Global sort: false
----------------

#-----------------------------------------------
# New Logical Plan:
#-----------------------------------------------
cleansed_by_genre: (Name: LOStore Schema: title#275:chararray,average_rating#296:double)
|
|---cleansed_by_genre: (Name: LOForEach Schema: title#275:chararray,average_rating#296:double)
    |   |
    |   (Name: LOGenerate[false,false] Schema: title#275:chararray,average_rating#296:double)ColumnPrune:OutputUids=[275, 296]ColumnPrune:InputUids=[275, 296]
    |   |   |
    |   |   filtered_by_genre::title:(Name: Project Type: chararray Uid: 275 Input: 0 Column: (*))
    |   |   |
    |   |   average_by_movie::avgRating:(Name: Project Type: double Uid: 296 Input: 1 Column: (*))
    |   |
    |   |---(Name: LOInnerLoad[3] Schema: filtered_by_genre::title#275:chararray)
    |   |
    |   |---(Name: LOInnerLoad[1] Schema: average_by_movie::avgRating#296:double)
    |
    |---joined_by_movie: (Name: LOJoin(HASH) Schema: average_by_movie::movieId#244:int,average_by_movie::avgRating#296:double,filtered_by_genre::movieId#274:int,filtered_by_genre::title#275:chararray,filtered_by_genre::genres#276:chararray)
        |   |
        |   movieId:(Name: Project Type: int Uid: 244 Input: 0 Column: 0)
        |   |
        |   movieId:(Name: Project Type: int Uid: 274 Input: 1 Column: 0)
        |
        |---average_by_movie: (Name: LOForEach Schema: movieId#244:int,avgRating#296:double)
        |   |   |
        |   |   (Name: LOGenerate[false,false] Schema: movieId#244:int,avgRating#296:double)ColumnPrune:OutputUids=[244, 296]ColumnPrune:InputUids=[244, 292]
        |   |   |   |
        |   |   |   group:(Name: Project Type: int Uid: 244 Input: 0 Column: (*))
        |   |   |   |
        |   |   |   (Name: UserFunc(org.apache.pig.builtin.FloatAvg) Type: double Uid: 296)
        |   |   |   |
        |   |   |   |---(Name: Dereference Type: bag Uid: 295 Column:[2])
        |   |   |       |
        |   |   |       |---(Name: Project Type: bag Uid: 292 Input: 1 Column: (*))
        |   |   |
        |   |   |---(Name: LOInnerLoad[0] Schema: group#244:int)
        |   |   |
        |   |   |---(Name: LOInnerLoad[1] Schema: userId#243:int,movieId#244:int,rating#245:float,timestamp#246:long)
        |   |
        |   |---grouped_by_movie: (Name: LOCogroup Schema: group#244:int,B#292:bag{#319:tuple(userId#243:int,movieId#244:int,rating#245:float,timestamp#246:long)})
        |       |   |
        |       |   movieId:(Name: Project Type: int Uid: 244 Input: 0 Column: 1)
        |       |
        |       |---B: (Name: LOFilter Schema: userId#243:int,movieId#244:int,rating#245:float,timestamp#246:long)
        |           |   |
        |           |   (Name: And Type: boolean Uid: 318)
        |           |   |
        |           |   |---(Name: GreaterThan Type: boolean Uid: 287)
        |           |   |   |
        |           |   |   |---(Name: Cast Type: double Uid: 245)
        |           |   |   |   |
        |           |   |   |   |---rating:(Name: Project Type: float Uid: 245 Input: 0 Column: 2)
        |           |   |   |
        |           |   |   |---(Name: Constant Type: double Uid: 286)
        |           |   |
        |           |   |---(Name: LessThan Type: boolean Uid: 290)
        |           |       |
        |           |       |---(Name: Cast Type: double Uid: 245)
        |           |       |   |
        |           |       |   |---rating:(Name: Project Type: float Uid: 245 Input: 0 Column: 2)
        |           |       |
        |           |       |---(Name: Constant Type: double Uid: 289)
        |           |
        |           |---A: (Name: LOForEach Schema: userId#243:int,movieId#244:int,rating#245:float,timestamp#246:long)
        |               |   |
        |               |   (Name: LOGenerate[false,false,false,false] Schema: userId#243:int,movieId#244:int,rating#245:float,timestamp#246:long)ColumnPrune:OutputUids=[243, 244, 245, 246]ColumnPrune:InputUids=[243, 244, 245, 246]
        |               |   |   |
        |               |   |   (Name: Cast Type: int Uid: 243)
        |               |   |   |
        |               |   |   |---userId:(Name: Project Type: bytearray Uid: 243 Input: 0 Column: (*))
        |               |   |   |
        |               |   |   (Name: Cast Type: int Uid: 244)
        |               |   |   |
        |               |   |   |---movieId:(Name: Project Type: bytearray Uid: 244 Input: 1 Column: (*))
        |               |   |   |
        |               |   |   (Name: Cast Type: float Uid: 245)
        |               |   |   |
        |               |   |   |---rating:(Name: Project Type: bytearray Uid: 245 Input: 2 Column: (*))
        |               |   |   |
        |               |   |   (Name: Cast Type: long Uid: 246)
        |               |   |   |
        |               |   |   |---timestamp:(Name: Project Type: bytearray Uid: 246 Input: 3 Column: (*))
        |               |   |
        |               |   |---(Name: LOInnerLoad[0] Schema: userId#243:bytearray)
        |               |   |
        |               |   |---(Name: LOInnerLoad[1] Schema: movieId#244:bytearray)
        |               |   |
        |               |   |---(Name: LOInnerLoad[2] Schema: rating#245:bytearray)
        |               |   |
        |               |   |---(Name: LOInnerLoad[3] Schema: timestamp#246:bytearray)
        |               |
        |               |---A: (Name: LOLoad Schema: userId#243:bytearray,movieId#244:bytearray,rating#245:bytearray,timestamp#246:bytearray)RequiredFields:null
        |
        |---filtered_by_genre: (Name: LOFilter Schema: movieId#274:int,title#275:chararray,genres#276:chararray)
            |   |
            |   (Name: Regex Type: boolean Uid: 317)
            |   |
            |   |---genres:(Name: Project Type: chararray Uid: 276 Input: 0 Column: 2)
            |   |
            |   |---(Name: Constant Type: chararray Uid: 316)
            |
            |---movies: (Name: LOForEach Schema: movieId#274:int,title#275:chararray,genres#276:chararray)
                |   |
                |   (Name: LOGenerate[false,false,false] Schema: movieId#274:int,title#275:chararray,genres#276:chararray)ColumnPrune:OutputUids=[274, 275, 276]ColumnPrune:InputUids=[274, 275, 276]
                |   |   |
                |   |   (Name: Cast Type: int Uid: 274)
                |   |   |
                |   |   |---movieId:(Name: Project Type: bytearray Uid: 274 Input: 0 Column: (*))
                |   |   |
                |   |   (Name: Cast Type: chararray Uid: 275)
                |   |   |
                |   |   |---title:(Name: Project Type: bytearray Uid: 275 Input: 1 Column: (*))
                |   |   |
                |   |   (Name: Cast Type: chararray Uid: 276)
                |   |   |
                |   |   |---genres:(Name: Project Type: bytearray Uid: 276 Input: 2 Column: (*))
                |   |
                |   |---(Name: LOInnerLoad[0] Schema: movieId#274:bytearray)
                |   |
                |   |---(Name: LOInnerLoad[1] Schema: title#275:bytearray)
                |   |
                |   |---(Name: LOInnerLoad[2] Schema: genres#276:bytearray)
                |
                |---movies: (Name: LOLoad Schema: movieId#274:bytearray,title#275:bytearray,genres#276:bytearray)RequiredFields:null
#-----------------------------------------------
# Physical Plan:
#-----------------------------------------------
cleansed_by_genre: Store(fakefile:org.apache.pig.builtin.PigStorage) - scope-227
|
|---cleansed_by_genre: New For Each(false,false)[bag] - scope-226
    |   |
    |   Project[chararray][3] - scope-222
    |   |
    |   Project[double][1] - scope-224
    |
    |---joined_by_movie: New For Each(true,true)[tuple] - scope-221
        |   |
        |   Project[bag][1] - scope-219
        |   |
        |   Project[bag][2] - scope-220
        |
        |---joined_by_movie: Package(Packager)[tuple]{int} - scope-214
            |
            |---joined_by_movie: Global Rearrange[tuple] - scope-213
                |
                |---joined_by_movie: Local Rearrange[tuple]{int}(false) - scope-215
                |   |   |
                |   |   Project[int][0] - scope-216
                |   |
                |   |---average_by_movie: New For Each(false,false)[bag] - scope-195
                |       |   |
                |       |   Project[int][0] - scope-189
                |       |   |
                |       |   POUserFunc(org.apache.pig.builtin.FloatAvg)[double] - scope-193
                |       |   |
                |       |   |---Project[bag][2] - scope-192
                |       |       |
                |       |       |---Project[bag][1] - scope-191
                |       |
                |       |---grouped_by_movie: Package(Packager)[tuple]{int} - scope-186
                |           |
                |           |---grouped_by_movie: Global Rearrange[tuple] - scope-185
                |               |
                |               |---grouped_by_movie: Local Rearrange[tuple]{int}(false) - scope-187
                |                   |   |
                |                   |   Project[int][1] - scope-188
                |                   |
                |                   |---B: Filter[bag] - scope-175
                |                       |   |
                |                       |   And[boolean] - scope-184
                |                       |   |
                |                       |   |---Greater Than[boolean] - scope-179
                |                       |   |   |
                |                       |   |   |---Cast[double] - scope-177
                |                       |   |   |   |
                |                       |   |   |   |---Project[float][2] - scope-176
                |                       |   |   |
                |                       |   |   |---Constant(0.5) - scope-178
                |                       |   |
                |                       |   |---Less Than[boolean] - scope-183
                |                       |       |
                |                       |       |---Cast[double] - scope-181
                |                       |       |   |
                |                       |       |   |---Project[float][2] - scope-180
                |                       |       |
                |                       |       |---Constant(5.0) - scope-182
                |                       |
                |                       |---A: New For Each(false,false,false,false)[bag] - scope-174
                |                           |   |
                |                           |   Cast[int] - scope-163
                |                           |   |
                |                           |   |---Project[bytearray][0] - scope-162
                |                           |   |
                |                           |   Cast[int] - scope-166
                |                           |   |
                |                           |   |---Project[bytearray][1] - scope-165
                |                           |   |
                |                           |   Cast[float] - scope-169
                |                           |   |
                |                           |   |---Project[bytearray][2] - scope-168
                |                           |   |
                |                           |   Cast[long] - scope-172
                |                           |   |
                |                           |   |---Project[bytearray][3] - scope-171
                |                           |
                |                           |---A: Load(/home/mkobbi/workspace/loana_lab2_mkobbi/ml-20m/ratings.csv:PigStorage(',')) - scope-161
                |
                |---joined_by_movie: Local Rearrange[tuple]{int}(false) - scope-217
                    |   |
                    |   Project[int][0] - scope-218
                    |
                    |---filtered_by_genre: Filter[bag] - scope-207
                        |   |
                        |   Matches - scope-210
                        |   |
                        |   |---Project[chararray][2] - scope-208
                        |   |
                        |   |---Constant(.*Documentary.*) - scope-209
                        |
                        |---movies: New For Each(false,false,false)[bag] - scope-206
                            |   |
                            |   Cast[int] - scope-198
                            |   |
                            |   |---Project[bytearray][0] - scope-197
                            |   |
                            |   Cast[chararray] - scope-201
                            |   |
                            |   |---Project[bytearray][1] - scope-200
                            |   |
                            |   Cast[chararray] - scope-204
                            |   |
                            |   |---Project[bytearray][2] - scope-203
                            |
                            |---movies: Load(/home/mkobbi/workspace/loana_lab2_mkobbi/ml-20m/movies.csv:PigStorage(',')) - scope-196

#--------------------------------------------------
# Map Reduce Plan                                  
#--------------------------------------------------
MapReduce node scope-228
Map Plan
grouped_by_movie: Local Rearrange[tuple]{int}(false) - scope-247
|   |
|   Project[int][0] - scope-249
|
|---average_by_movie: New For Each(false,false)[bag] - scope-235
    |   |
    |   Project[int][0] - scope-236
    |   |
    |   POUserFunc(org.apache.pig.builtin.FloatAvg$Initial)[tuple] - scope-237
    |   |
    |   |---Project[bag][2] - scope-238
    |       |
    |       |---Project[bag][1] - scope-239
    |
    |---Pre Combiner Local Rearrange[tuple]{Unknown} - scope-250
        |
        |---B: Filter[bag] - scope-175
            |   |
            |   And[boolean] - scope-184
            |   |
            |   |---Greater Than[boolean] - scope-179
            |   |   |
            |   |   |---Cast[double] - scope-177
            |   |   |   |
            |   |   |   |---Project[float][2] - scope-176
            |   |   |
            |   |   |---Constant(0.5) - scope-178
            |   |
            |   |---Less Than[boolean] - scope-183
            |       |
            |       |---Cast[double] - scope-181
            |       |   |
            |       |   |---Project[float][2] - scope-180
            |       |
            |       |---Constant(5.0) - scope-182
            |
            |---A: New For Each(false,false,false,false)[bag] - scope-174
                |   |
                |   Cast[int] - scope-163
                |   |
                |   |---Project[bytearray][0] - scope-162
                |   |
                |   Cast[int] - scope-166
                |   |
                |   |---Project[bytearray][1] - scope-165
                |   |
                |   Cast[float] - scope-169
                |   |
                |   |---Project[bytearray][2] - scope-168
                |   |
                |   Cast[long] - scope-172
                |   |
                |   |---Project[bytearray][3] - scope-171
                |
                |---A: Load(/home/mkobbi/workspace/loana_lab2_mkobbi/ml-20m/ratings.csv:PigStorage(',')) - scope-161--------
Combine Plan
grouped_by_movie: Local Rearrange[tuple]{int}(false) - scope-251
|   |
|   Project[int][0] - scope-253
|
|---average_by_movie: New For Each(false,false)[bag] - scope-240
    |   |
    |   Project[int][0] - scope-241
    |   |
    |   POUserFunc(org.apache.pig.builtin.FloatAvg$Intermediate)[tuple] - scope-242
    |   |
    |   |---Project[bag][1] - scope-243
    |
    |---grouped_by_movie: Package(CombinerPackager)[tuple]{int} - scope-246--------
Reduce Plan
Store(file:/tmp/temp948730864/tmp2045036720:org.apache.pig.impl.io.InterStorage) - scope-229
|
|---average_by_movie: New For Each(false,false)[bag] - scope-195
    |   |
    |   Project[int][0] - scope-189
    |   |
    |   POUserFunc(org.apache.pig.builtin.FloatAvg$Final)[double] - scope-193
    |   |
    |   |---Project[bag][1] - scope-244
    |
    |---grouped_by_movie: Package(CombinerPackager)[tuple]{int} - scope-186--------
Global sort: false
----------------

MapReduce node scope-233
Map Plan
Union[tuple] - scope-234
|
|---joined_by_movie: Local Rearrange[tuple]{int}(false) - scope-215
|   |   |
|   |   Project[int][0] - scope-216
|   |
|   |---Load(file:/tmp/temp948730864/tmp2045036720:org.apache.pig.impl.io.InterStorage) - scope-230
|
|---joined_by_movie: Local Rearrange[tuple]{int}(false) - scope-217
    |   |
    |   Project[int][0] - scope-218
    |
    |---filtered_by_genre: Filter[bag] - scope-207
        |   |
        |   Matches - scope-210
        |   |
        |   |---Project[chararray][2] - scope-208
        |   |
        |   |---Constant(.*Documentary.*) - scope-209
        |
        |---movies: New For Each(false,false,false)[bag] - scope-206
            |   |
            |   Cast[int] - scope-198
            |   |
            |   |---Project[bytearray][0] - scope-197
            |   |
            |   Cast[chararray] - scope-201
            |   |
            |   |---Project[bytearray][1] - scope-200
            |   |
            |   Cast[chararray] - scope-204
            |   |
            |   |---Project[bytearray][2] - scope-203
            |
            |---movies: Load(/home/mkobbi/workspace/loana_lab2_mkobbi/ml-20m/movies.csv:PigStorage(',')) - scope-196--------
Reduce Plan
cleansed_by_genre: Store(fakefile:org.apache.pig.builtin.PigStorage) - scope-227
|
|---cleansed_by_genre: New For Each(false,false)[bag] - scope-226
    |   |
    |   Project[chararray][3] - scope-222
    |   |
    |   Project[double][1] - scope-224
    |
    |---joined_by_movie: Package(JoinPackager(true,true))[tuple]{int} - scope-214--------
Global sort: false
----------------

#-----------------------------------------------
# New Logical Plan:
#-----------------------------------------------
cleansed_by_action: (Name: LOStore Schema: title#399:chararray,count#422:long)
|
|---cleansed_by_action: (Name: LOForEach Schema: title#399:chararray,count#422:long)
    |   |
    |   (Name: LOGenerate[false,false] Schema: title#399:chararray,count#422:long)ColumnPrune:OutputUids=[422, 399]ColumnPrune:InputUids=[422, 399]
    |   |   |
    |   |   filtered_by_action::title:(Name: Project Type: chararray Uid: 399 Input: 0 Column: (*))
    |   |   |
    |   |   movie_tags_distinct::tags_count:(Name: Project Type: long Uid: 422 Input: 1 Column: (*))
    |   |
    |   |---(Name: LOInnerLoad[3] Schema: filtered_by_action::title#399:chararray)
    |   |
    |   |---(Name: LOInnerLoad[1] Schema: movie_tags_distinct::tags_count#422:long)
    |
    |---tag_movie_join: (Name: LOJoin(HASH) Schema: movie_tags_distinct::movieId#404:int,movie_tags_distinct::tags_count#422:long,filtered_by_action::movieId#398:int,filtered_by_action::title#399:chararray,filtered_by_action::genres#400:chararray)
        |   |
        |   movieId:(Name: Project Type: int Uid: 404 Input: 0 Column: 0)
        |   |
        |   movieId:(Name: Project Type: int Uid: 398 Input: 1 Column: 0)
        |
        |---movie_tags_distinct: (Name: LOForEach Schema: movieId#404:int,tags_count#422:long)
        |   |   |
        |   |   (Name: LOGenerate[false,false] Schema: movieId#404:int,tags_count#422:long)ColumnPrune:OutputUids=[404, 422]ColumnPrune:InputUids=[418, 404]
        |   |   |   |
        |   |   |   group:(Name: Project Type: int Uid: 404 Input: 0 Column: (*))
        |   |   |   |
        |   |   |   (Name: UserFunc(org.apache.pig.builtin.COUNT_STAR) Type: long Uid: 422)
        |   |   |   |
        |   |   |   |---unique_segments:(Name: Project Type: bag Uid: 421 Input: 1 Column: (*))
        |   |   |
        |   |   |---(Name: LOInnerLoad[0] Schema: group#404:int)
        |   |   |
        |   |   |---unique_segments: (Name: LODistinct Schema: tag#405:chararray)
        |   |       |
        |   |       |---1-34: (Name: LOForEach Schema: tag#405:chararray)
        |   |           |   |
        |   |           |   (Name: LOGenerate[false] Schema: tag#405:chararray)
        |   |           |   |   |
        |   |           |   |   tag:(Name: Project Type: chararray Uid: 405 Input: 0 Column: (*))
        |   |           |   |
        |   |           |   |---(Name: LOInnerLoad[2] Schema: tag#405:chararray)
        |   |           |
        |   |           |---tags: (Name: LOInnerLoad[1] Schema: userId#403:int,movieId#404:int,tag#405:chararray,timestamp#406:long)
        |   |
        |   |---movie_tags_all: (Name: LOCogroup Schema: group#404:int,tags#418:bag{#438:tuple(userId#403:int,movieId#404:int,tag#405:chararray,timestamp#406:long)})
        |       |   |
        |       |   movieId:(Name: Project Type: int Uid: 404 Input: 0 Column: 1)
        |       |
        |       |---tags: (Name: LOForEach Schema: userId#403:int,movieId#404:int,tag#405:chararray,timestamp#406:long)
        |           |   |
        |           |   (Name: LOGenerate[false,false,false,false] Schema: userId#403:int,movieId#404:int,tag#405:chararray,timestamp#406:long)ColumnPrune:OutputUids=[403, 404, 405, 406]ColumnPrune:InputUids=[403, 404, 405, 406]
        |           |   |   |
        |           |   |   (Name: Cast Type: int Uid: 403)
        |           |   |   |
        |           |   |   |---userId:(Name: Project Type: bytearray Uid: 403 Input: 0 Column: (*))
        |           |   |   |
        |           |   |   (Name: Cast Type: int Uid: 404)
        |           |   |   |
        |           |   |   |---movieId:(Name: Project Type: bytearray Uid: 404 Input: 1 Column: (*))
        |           |   |   |
        |           |   |   (Name: Cast Type: chararray Uid: 405)
        |           |   |   |
        |           |   |   |---tag:(Name: Project Type: bytearray Uid: 405 Input: 2 Column: (*))
        |           |   |   |
        |           |   |   (Name: Cast Type: long Uid: 406)
        |           |   |   |
        |           |   |   |---timestamp:(Name: Project Type: bytearray Uid: 406 Input: 3 Column: (*))
        |           |   |
        |           |   |---(Name: LOInnerLoad[0] Schema: userId#403:bytearray)
        |           |   |
        |           |   |---(Name: LOInnerLoad[1] Schema: movieId#404:bytearray)
        |           |   |
        |           |   |---(Name: LOInnerLoad[2] Schema: tag#405:bytearray)
        |           |   |
        |           |   |---(Name: LOInnerLoad[3] Schema: timestamp#406:bytearray)
        |           |
        |           |---tags: (Name: LOLoad Schema: userId#403:bytearray,movieId#404:bytearray,tag#405:bytearray,timestamp#406:bytearray)RequiredFields:null
        |
        |---filtered_by_action: (Name: LOFilter Schema: movieId#398:int,title#399:chararray,genres#400:chararray)
            |   |
            |   (Name: Regex Type: boolean Uid: 442)
            |   |
            |   |---genres:(Name: Project Type: chararray Uid: 400 Input: 0 Column: 2)
            |   |
            |   |---(Name: Constant Type: chararray Uid: 441)
            |
            |---movies: (Name: LOForEach Schema: movieId#398:int,title#399:chararray,genres#400:chararray)
                |   |
                |   (Name: LOGenerate[false,false,false] Schema: movieId#398:int,title#399:chararray,genres#400:chararray)ColumnPrune:OutputUids=[400, 398, 399]ColumnPrune:InputUids=[400, 398, 399]
                |   |   |
                |   |   (Name: Cast Type: int Uid: 398)
                |   |   |
                |   |   |---movieId:(Name: Project Type: bytearray Uid: 398 Input: 0 Column: (*))
                |   |   |
                |   |   (Name: Cast Type: chararray Uid: 399)
                |   |   |
                |   |   |---title:(Name: Project Type: bytearray Uid: 399 Input: 1 Column: (*))
                |   |   |
                |   |   (Name: Cast Type: chararray Uid: 400)
                |   |   |
                |   |   |---genres:(Name: Project Type: bytearray Uid: 400 Input: 2 Column: (*))
                |   |
                |   |---(Name: LOInnerLoad[0] Schema: movieId#398:bytearray)
                |   |
                |   |---(Name: LOInnerLoad[1] Schema: title#399:bytearray)
                |   |
                |   |---(Name: LOInnerLoad[2] Schema: genres#400:bytearray)
                |
                |---movies: (Name: LOLoad Schema: movieId#398:bytearray,title#399:bytearray,genres#400:bytearray)RequiredFields:null
#-----------------------------------------------
# Physical Plan:
#-----------------------------------------------
cleansed_by_action: Store(fakefile:org.apache.pig.builtin.PigStorage) - scope-314
|
|---cleansed_by_action: New For Each(false,false)[bag] - scope-313
    |   |
    |   Project[chararray][3] - scope-309
    |   |
    |   Project[long][1] - scope-311
    |
    |---tag_movie_join: New For Each(true,true)[tuple] - scope-308
        |   |
        |   Project[bag][1] - scope-306
        |   |
        |   Project[bag][2] - scope-307
        |
        |---tag_movie_join: Package(Packager)[tuple]{int} - scope-301
            |
            |---tag_movie_join: Global Rearrange[tuple] - scope-300
                |
                |---tag_movie_join: Local Rearrange[tuple]{int}(false) - scope-302
                |   |   |
                |   |   Project[int][0] - scope-303
                |   |
                |   |---movie_tags_distinct: New For Each(false,false)[bag] - scope-282
                |       |   |
                |       |   Project[int][0] - scope-273
                |       |   |
                |       |   POUserFunc(org.apache.pig.builtin.COUNT_STAR)[long] - scope-276
                |       |   |
                |       |   |---RelationToExpressionProject[bag][*] - scope-275
                |       |       |
                |       |       |---unique_segments: PODistinct[bag] - scope-281
                |       |           |
                |       |           |---1-34: New For Each(false)[bag] - scope-280
                |       |               |   |
                |       |               |   Project[chararray][2] - scope-278
                |       |               |
                |       |               |---Project[bag][1] - scope-277
                |       |
                |       |---movie_tags_all: Package(Packager)[tuple]{int} - scope-270
                |           |
                |           |---movie_tags_all: Global Rearrange[tuple] - scope-269
                |               |
                |               |---movie_tags_all: Local Rearrange[tuple]{int}(false) - scope-271
                |                   |   |
                |                   |   Project[int][1] - scope-272
                |                   |
                |                   |---tags: New For Each(false,false,false,false)[bag] - scope-268
                |                       |   |
                |                       |   Cast[int] - scope-257
                |                       |   |
                |                       |   |---Project[bytearray][0] - scope-256
                |                       |   |
                |                       |   Cast[int] - scope-260
                |                       |   |
                |                       |   |---Project[bytearray][1] - scope-259
                |                       |   |
                |                       |   Cast[chararray] - scope-263
                |                       |   |
                |                       |   |---Project[bytearray][2] - scope-262
                |                       |   |
                |                       |   Cast[long] - scope-266
                |                       |   |
                |                       |   |---Project[bytearray][3] - scope-265
                |                       |
                |                       |---tags: Load(/home/mkobbi/workspace/loana_lab2_mkobbi/ml-20m/tags.csv:PigStorage(',')) - scope-255
                |
                |---tag_movie_join: Local Rearrange[tuple]{int}(false) - scope-304
                    |   |
                    |   Project[int][0] - scope-305
                    |
                    |---filtered_by_action: Filter[bag] - scope-294
                        |   |
                        |   Matches - scope-297
                        |   |
                        |   |---Project[chararray][2] - scope-295
                        |   |
                        |   |---Constant(.*Action.*) - scope-296
                        |
                        |---movies: New For Each(false,false,false)[bag] - scope-293
                            |   |
                            |   Cast[int] - scope-285
                            |   |
                            |   |---Project[bytearray][0] - scope-284
                            |   |
                            |   Cast[chararray] - scope-288
                            |   |
                            |   |---Project[bytearray][1] - scope-287
                            |   |
                            |   Cast[chararray] - scope-291
                            |   |
                            |   |---Project[bytearray][2] - scope-290
                            |
                            |---movies: Load(/home/mkobbi/workspace/loana_lab2_mkobbi/ml-20m/movies.csv:PigStorage(',')) - scope-283

#--------------------------------------------------
# Map Reduce Plan                                  
#--------------------------------------------------
MapReduce node scope-315
Map Plan
movie_tags_all: Local Rearrange[tuple]{int}(false) - scope-336
|   |
|   Project[int][0] - scope-338
|
|---movie_tags_distinct: New For Each(false,false)[bag] - scope-323
    |   |
    |   Project[int][0] - scope-324
    |   |
    |   POUserFunc(org.apache.pig.builtin.Distinct$Initial)[tuple] - scope-325
    |   |
    |   |---1-34: New For Each(false)[tuple] - scope-327
    |       |   |
    |       |   Project[chararray][2] - scope-326
    |       |
    |       |---Project[bag][1] - scope-328
    |
    |---Pre Combiner Local Rearrange[tuple]{Unknown} - scope-339
        |
        |---tags: New For Each(false,false,false,false)[bag] - scope-268
            |   |
            |   Cast[int] - scope-257
            |   |
            |   |---Project[bytearray][0] - scope-256
            |   |
            |   Cast[int] - scope-260
            |   |
            |   |---Project[bytearray][1] - scope-259
            |   |
            |   Cast[chararray] - scope-263
            |   |
            |   |---Project[bytearray][2] - scope-262
            |   |
            |   Cast[long] - scope-266
            |   |
            |   |---Project[bytearray][3] - scope-265
            |
            |---tags: Load(/home/mkobbi/workspace/loana_lab2_mkobbi/ml-20m/tags.csv:PigStorage(',')) - scope-255--------
Combine Plan
movie_tags_all: Local Rearrange[tuple]{int}(false) - scope-340
|   |
|   Project[int][0] - scope-342
|
|---movie_tags_distinct: New For Each(false,false)[bag] - scope-329
    |   |
    |   Project[int][0] - scope-330
    |   |
    |   POUserFunc(org.apache.pig.builtin.Distinct$Intermediate)[tuple] - scope-331
    |   |
    |   |---Project[bag][1] - scope-332
    |
    |---movie_tags_all: Package(CombinerPackager)[tuple]{int} - scope-335--------
Reduce Plan
Store(file:/tmp/temp948730864/tmp1633676042:org.apache.pig.impl.io.InterStorage) - scope-316
|
|---movie_tags_distinct: New For Each(false,false)[bag] - scope-282
    |   |
    |   Project[int][0] - scope-273
    |   |
    |   POUserFunc(org.apache.pig.builtin.COUNT_STAR)[long] - scope-276
    |   |
    |   |---POUserFunc(org.apache.pig.builtin.Distinct$Final)[bag] - scope-322
    |       |
    |       |---Project[bag][1] - scope-333
    |
    |---movie_tags_all: Package(CombinerPackager)[tuple]{int} - scope-270--------
Global sort: false
----------------

MapReduce node scope-320
Map Plan
Union[tuple] - scope-321
|
|---tag_movie_join: Local Rearrange[tuple]{int}(false) - scope-302
|   |   |
|   |   Project[int][0] - scope-303
|   |
|   |---Load(file:/tmp/temp948730864/tmp1633676042:org.apache.pig.impl.io.InterStorage) - scope-317
|
|---tag_movie_join: Local Rearrange[tuple]{int}(false) - scope-304
    |   |
    |   Project[int][0] - scope-305
    |
    |---filtered_by_action: Filter[bag] - scope-294
        |   |
        |   Matches - scope-297
        |   |
        |   |---Project[chararray][2] - scope-295
        |   |
        |   |---Constant(.*Action.*) - scope-296
        |
        |---movies: New For Each(false,false,false)[bag] - scope-293
            |   |
            |   Cast[int] - scope-285
            |   |
            |   |---Project[bytearray][0] - scope-284
            |   |
            |   Cast[chararray] - scope-288
            |   |
            |   |---Project[bytearray][1] - scope-287
            |   |
            |   Cast[chararray] - scope-291
            |   |
            |   |---Project[bytearray][2] - scope-290
            |
            |---movies: Load(/home/mkobbi/workspace/loana_lab2_mkobbi/ml-20m/movies.csv:PigStorage(',')) - scope-283--------
Reduce Plan
cleansed_by_action: Store(fakefile:org.apache.pig.builtin.PigStorage) - scope-314
|
|---cleansed_by_action: New For Each(false,false)[bag] - scope-313
    |   |
    |   Project[chararray][3] - scope-309
    |   |
    |   Project[long][1] - scope-311
    |
    |---tag_movie_join: Package(JoinPackager(true,true))[tuple]{int} - scope-301--------
Global sort: false
----------------

