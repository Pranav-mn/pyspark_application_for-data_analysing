import os

os.environ['envn']='DEV'
os.environ['header']='True'
os.environ['inferSchema']='True'

header=os.environ['header']
inferSchema=os.environ['header']
envn=os.environ['envn']

appName='pyspark project'

current=os.getcwd()

src_olap=current+'/source/olap'
src_oltp=current+'/source/oltp'