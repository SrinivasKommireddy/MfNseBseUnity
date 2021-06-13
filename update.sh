# !/bin/bash

# check internet availability
  ping -c 1 www.google.com
  PING_STATUS=$?
  if [[ $PING_STATUS -ne 0 ]]
    then
      echo "RC=" ${PING_STATUS} ", Internet not accessible, exiting"
      exit 2
    else
      true
  fi

# check postgres availability
  psql --username=postgres --host=localhost --dbname=unity -c '\q'
  POSTGRES_STATUS=$?
  if [[ $POSTGRES_STATUS -ne 0 ]]
    then
      echo "Postgres is possibly not running, exiting"
      exit 1
    else
      true
  fi

cd /home/kommire/eq
cat '' > wget.log

for MARKET in idx nse bse mf
do
  echo MARKET=$MARKET
	date
	bash sc.url_gen.sh $MARKET
	date
	bash sc.url_dwld.sh $MARKET
	date
	bash sc.process_convert.sh $MARKET
	date
	bash sc.process_gap_fix.sh $MARKET
	date
done

for MARKET in idx nse bse mf
do
  bash sc.upload.sh $MARKET
done

#cd /home/kommire/hist5m
#
#for MARKET in nse bse
#do
#  echo MARKET=$MARKET
#	bash sc.gen_url.sh $MARKET
#	bash sc.url_dwld.sh $MARKET
#	bash sc.process_convert.sh $MARKET
#	bash sc.process_gap_fix.sh $MARKET
#done
#
#for MARKET in nse bse
#do
#  bash sc.upload.sh $MARKET
#done
