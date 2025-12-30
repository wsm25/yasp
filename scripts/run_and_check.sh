python ./genconfig.py
echo Check on low pressure
python ./run.py 1 100 5
python ./check.py 5
echo Check on high pressure
python ./run.py 1 10000 5
python ./check.py 5