import os
import sys
import pyspark
import string
import csv
import json
import statistics
from itertools import combinations
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql import types as D
from pyspark.sql.window import Window
from dateutil.parser import parse
import datetime

spark = SparkSession.builder.appName("project-part1").config("spark.some.config.option", "some-value").getOrCreate()

if not os.path.exists('Results_JSON'):
    os.makedirs('Results_JSON')

filelist=os.listdir('NYCOpenData/')
filelist1=[]
file_name_dict={}
with open('NYCOpenData/datasets.tsv') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    for i,row in enumerate(reader):
        file_name_dict[row[0].lower()]=row[1]
        filelist1.append(row[0]+'.tsv.gz')

filelist.remove('datasets.tsv')
filelist1=filelist1[580:]
def count_not_null(c, nan_as_null=False):
    pred = F.col(c).isNotNull() & (~isnan(c) if nan_as_null else F.lit(True))
    return F.sum(pred.cast("integer")).alias(c)

get_integer=F.udf(lambda x: x if type(x)==int else None, D.IntegerType())
get_string=F.udf(lambda x: x if type(x)==str else None, D.StringType())
get_float=F.udf(lambda x: x if type(x)==float else None, D.FloatType())

def check_date(d):
    try:
        z=parse(d)
        return str(z)
    except:
        return None

def str_to_int(d):
    if type(d)==str:
        try:
            z=int(d)
            return z
        except:
            return None
    else:
        return None
    
def str_to_float(d):
    if type(d)==str:
        try:
            z=float(d)
            return z
        except:
            return None
    else:
        return None
get_date=F.udf(lambda x: check_date(x), D.StringType())
get_str_int=F.udf(lambda x: str_to_int(x), D.IntegerType())
get_str_float=F.udf(lambda x: str_to_float(x), D.FloatType())

c=1
filelist1=['b36s-tuqc', 'ymvu-4x4s', 'hg3k-2w6h', 'y4yc-78a4', '6pwv-zmgh', 'hve5-8z68', 'hnj4-7uii', 'hm7r-w4y9', 'x2ak-gjxj', 's5mq-f5ie', 'mu33-54em', 'd4uz-6jaw', 'cgd2-a96k', '26ze-s5bx', 'tmha-56pf', 'au2v-djg4', '6dn9-qgma', 'k2wr-xdxp', 'jege-pgbz', 'vxrc-r6hh', 'rc2t-8fid', 'qt67-786k', 'dgg9-jkx8', 'yizy-365y', 'q9kp-jvxv', 'pf7i-ims3', 'f64t-5yiv', 'r63n-8hf3', 'mpqk-skis', 'hphp-39kk', 'eifs-wzhm', 'mpw3-7xyh', '6wve-ubwx', 'vr2i-c3qq', 'vgqq-m7ux', 'dn64-92ub', 'at9c-pdf7', 'yv4m-nu6d', 'a9es-3fcm', 's2zm-f47y', 'qt4e-9a97', 'tiyn-ajjm', '6yag-pnij', 'mrey-ykjt', 'ft9w-x7u5', 'ay6v-3gm3', 'r69u-62nw', 'h7mf-hrsw', '7m8q-jgtg', 'e266-vpg7', '2enn-s52j', 'x56h-7iwp', 'w5he-u64t', 'svyi-maaj', 'ws4r-n9v4', 'gk9u-c3tv', 'arhf-esqb', 'bj6m-ydtj', '483x-fy9e', '8qnp-qwn4', 'c87b-2j3i', 'nhca-vme6', '8kiv-2ukd', 'tbhc-a2hs', 'ivbu-e2q7', '5vqp-meve', 'fuwu-64db', '4se9-mk53', '4zdr-zwdi', 'i447-i5u3', 'pa5t-ktd3', '8fhd-nzw8', '7vk6-9w2d', 'qnwe-j5my', 'sci4-yqgk', 'ismp-xffj', 'cxcv-mgtn', 'wip6-ytad', '5t4n-d72c', 'vk9f-gvzq', 'gx64-vjdi', 'mpg8-b8s5', '4se9-u6dw', 'xqxn-4jph', 'azp6-hepu', '8zxg-9a5c', 'vpb3-uf7s', 'su6u-afcg', 'twvd-c9s3', '37fm-7uaa', 'ftyx-fhnc', 'nfz9-tzba', 'x4x8-m3ds', 'tjt7-rf2i', 'peat-vx5z', '2knz-3yg6', 'gffu-ps8j', 'iyka-7txw', 'mzy5-smmw', 'vbgf-ket3', 'aq7p-fpw6', 'fuhv-y2p6', '7zb8-7bpk', '3f5y-5web', 'uu87-uz8m', 'h6ku-kycw', '32yu-maz2', 'tbf6-u8ea', 'fsis-j6x5', '65js-fhgz', '2n4x-d97d', 'vypd-m9q8', 'az65-9z36', '6r8r-c474', '5zea-fjhf', '4kkh-qhtc', '4epu-t832', '9ev8-8rz6', '3955-c36a', '7ree-jtaa', 'v7hc-c85a', '3mji-gpg5', 'aqu8-c9ea', 'ng7s-zne2', '4fsz-s7id', 'ff9v-9yzg', '5jwd-xj5z', '3dnj-u3x8', '4f2g-zrdv', '5c9e-33xj', 'hdu7-ujt4', '3av7-txd8', 'mumm-6t9n', '2zdm-jpub', 'jpkr-jhk8', 's52s-navf', 'ph29-5mxy', 'ugc2-6t2g', '8dxm-n5ha', '5ps9-yuef', 'uwx4-aafe', 'kkwv-djnk', 'n7ta-pz8k', '7rf2-3gxf', 'uzf5-g4f3', 'rqgf-94xs', 'xs6e-ka4w', 'pz9j-6fgq', 'dqsy-6vth', 'ky6q-fvpr', 'snck-inhz', 'tzwr-vksx', 'jt9i-9gxr', 'b8z8-mpdb', 'sm48-t3s6', 'uh2w-zjsn', 'qtrj-g3nm', '6khm-nrue', 'hdnu-nbrh', 'sgvu-nui7', 'hjvj-jfc9', 'dtmw-avzj', 'ubdi-jgw2', '56u5-n9sa', '33db-aeds', 'myn9-hwsy', '97pn-acdf', 'cyfw-hfqk', '99gz-6gpw', 'pd6g-yghw', '6kks-jijx', 'xzy8-qqgf', '66yh-nemi', 'gbgg-xjuf', 'uedp-fegm', '3aka-ggej', 'e4ej-j6hn', '99br-frp6', 'f9a8-4jby', 'nmue-7zq2', 'cr93-x2xf', 'iwdd-99mu', 'wbtw-zkex', '7uuj-b95m', 'i3ez-z58g', 'p39r-nm7f', '3c9q-94ad', '9nyh-gsuu', 'fb6n-h22r', '2jne-kr3f', 'qpm9-j523', '93d2-wh7s', '39qw-754y', 'c3uy-2p5r', 'v475-8jcj', '9z9b-6hvk', 'it9k-rtx5', '5qbe-4vqd', '6246-94tp', 'axb2-9jkb', 'uep6-mri2', 'szn6-bbuk', '52b4-grs5', '9f25-223x', 'tiwv-ukz3', 'mrxp-4hmg', '5kqf-fg3n', '5ghg-dyn6', '799n-b76v', '9umc-3b2y', 'xp25-gxux', 'mq6n-s45c', 'nntm-ht5m', 'dbs9-fgqs', 'dzrn-z4d7', 'd9fi-geci', 'eg59-gdqu', '7xjx-2mhj', 'cm6g-t7ye', 'gpny-cuvw', '8bug-hj9w', 'd4iy-9uh7', 'i3a3-qxkf', 'ws4c-4g69', 'k26i-s5bd', 'd33y-i2m7', 'ykvb-493p', '7kc8-z939', 'hafw-ruje', '4hiy-398i']

filelist1=filelist1[139:]
print('files left = ', len(filelist1)) 
for file in filelist1:
    filepath='/user/hm74/NYCOpenData/'+file.lower()+'.tsv.gz'
    DF = spark.read.format('csv').options(header='true',inferschema='true').option("delimiter", "\t").load(filepath)
    DF_dict={"dataset_name": file_name_dict[file.split('.')[0]], 'columns': DF.columns, 'key_column_candidates':[]}
    col_name_list=DF.columns 
    col_dicts=[]
    total_rows=DF.count()
    
    for i, x in enumerate(DF.columns):
        DF=DF.withColumnRenamed(x, str(i))
    count_not_null_all_col = DF.agg(*[count_not_null(c) for c in DF.columns]).collect()[0]
    count_null_all_col=[(total_rows-count_notNull) for count_notNull in count_not_null_all_col]
    
     
    for i, cols in enumerate(DF.columns):
        if total_rows==0:
            continue
        col_dict={}
        col_dict["column_name"]=col_name_list[i]
        col_dict['number_non_empty_cells']=count_not_null_all_col[i]
        col_dict['number_empty_cells']=count_null_all_col[i]
        freq_DF_desc=DF.groupBy(cols).count().sort(F.desc('count'))
        freq_DF_desc=freq_DF_desc.where(F.col(cols).isNotNull())
        
        top5_freq=[]
        if freq_DF_desc.count()<5:
            top5_freq=[row[0] for row in freq_DF_desc.collect()]
        else:
            top5_freq=[row[0] for row in freq_DF_desc.take(5)]
        col_dict['frequent_values']=top5_freq
        col_dict['data_types']=[]
        
        int_col=cols+' '+'int_type'
        str_col=cols+' '+'str_type'
        float_col=cols+ ' '+ 'float_type'
        date_col=cols+' '+'date_type'
        str_int_col=cols + ' '+'str_int'
        str_float_col=cols +' '+'str_float'
        df=DF.select([get_integer(cols).alias(int_col), get_string(cols).alias(str_col), get_float(cols).alias(float_col), get_date(cols).alias(date_col),
                     get_str_int(cols).alias(str_int_col),get_str_float(cols).alias(str_float_col)])
        
        int_df=df.select(int_col).where(F.col(int_col).isNotNull())
        str_df=df.select(str_col).where(F.col(str_col).isNotNull())
        float_df=df.select(float_col).where(F.col(float_col).isNotNull())
        date_df=df.select(date_col).where(F.col(date_col).isNotNull())
        str_int_df=df.select(str_int_col).where(F.col(str_int_col).isNotNull())
        str_float_df=df.select(str_float_col).where(F.col(str_float_col).isNotNull())
        
        if float_df.count()>1:
            type_dict={}
            type_dict['type']='REAL'
            type_dict['count']=float_df.count()
            type_dict['max_value']=float_df.agg({float_col: "max"}).collect()[0][0]
            type_dict['min_value']=float_df.agg({float_col: "min"}).collect()[0][0]
            type_dict['mean']=float_df.agg({float_col: "avg"}).collect()[0][0]
            type_dict['stddev']=float_df.agg({float_col: 'stddev'}).collect()[0][0]
            col_dict['data_types'].append(type_dict)
        
        if int_df.count()>1:
            type_dict={}
            type_dict['type']='INTEGER (LONG)'
            type_dict['count']=int_df.count()
            type_dict['max_value']=int_df.agg({int_col: 'max'}).collect()[0][0]
            type_dict['min_value']=int_df.agg({int_col: 'min'}).collect()[0][0]
            type_dict['mean']=int_df.agg({int_col: 'avg'}).collect()[0][0]
            type_dict['stddev']=int_df.agg({int_col: 'stddev'}).collect()[0][0]
            col_dict['data_types'].append(type_dict)
            
        if str_df.count()>1:
            type_dict={'type':'TEXT', 'count': str_df.count()}
            str_rows=str_df.distinct().collect()
            str_arr=[row[0] for row in str_rows]
            if len(str_arr)<=5:
                type_dict['shortest_values']=str_arr
                type_dict['longest_values']=str_arr
                
            else:
                str_arr.sort(key=len, reverse=True)
                type_dict['shortest_values']=str_arr[-5:]
                type_dict['longest_values']=str_arr[:5]
            
            type_dict['average_length']=sum(map(len, str_arr))/len(str_arr)
            col_dict['data_types'].append(type_dict)
        
        if date_df.count()>1:
            type_dict={"type":"DATE/TIME", "count":date_df.count()}
            min_date, max_date = date_df.select(F.min(date_col), F.max(date_col)).first()
            type_dict['max_value']=max_date
            type_dict['min_value']=min_date
            col_dict['data_types'].append(type_dict)
        
        if str_float_df.count()>1:
            type_dict={}
            type_dict['type']='REAL'
            type_dict['count']=str_float_df.count()
            type_dict['max_value']=str_float_df.agg({str_float_col: "max"}).collect()[0][0]
            type_dict['min_value']=str_float_df.agg({str_float_col: "min"}).collect()[0][0]
            type_dict['mean']=str_float_df.agg({str_float_col: "avg"}).collect()[0][0]
            type_dict['stddev']=str_float_df.agg({str_float_col: 'stddev'}).collect()[0][0]
            col_dict['data_types'].append(type_dict)
        
        if str_int_df.count()>1:
            type_dict={}
            type_dict['type']='INTEGER (LONG)'
            type_dict['count']=str_int_df.count()
            type_dict['max_value']=str_int_df.agg({str_int_col: 'max'}).collect()[0][0]
            type_dict['min_value']=str_int_df.agg({str_int_col: 'min'}).collect()[0][0]
            type_dict['mean']=str_int_df.agg({str_int_col: 'avg'}).collect()[0][0]
            type_dict['stddev']=str_int_df.agg({str_int_col: 'stddev'}).collect()[0][0]
            col_dict['data_types'].append(type_dict)
        col_dicts.append(col_dict)
    
    
    json_file_path=file.split('.')[0]
    print('Processed '+ str(c)+' file')
    c=c+1
    json_file_path='Results_JSON/'+ json_file_path
    with open(json_file_path, 'w', newline='\n') as json_file:
        json.dump(DF_dict, json_file)
        for Dict in col_dicts:
            json.dump(Dict, json_file,default=str)
    
    
            
      


