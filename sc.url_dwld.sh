# !/bin/bash

  source sc.env.sh
#
  rm in/${MKT}.*.* || true
  rm wget.log || true
  rm todo.*.${URL_FILE} || true
  rm comp.*.${URL_FILE} || true
  rm tmp.todo.*.${URL_FILE} || true
  rm tmp.*.$0.threads.sh    || true

  TODO_FILE=tmp.$PID.todo.${URL_FILE}; cp ${URL_FILE} $TODO_FILE
  COMP_FILE=tmp.$PID.comp.${URL_FILE}; touch $COMP_FILE
  SH_FILE=tmp.$PID.$0.threads.sh
  
  OLDIFS=$IFS
  IFS=$'\n'
# wget the urls
  while [[ $(cat $TODO_FILE | wc -l) -gt 0 ]]
    do
      # create sh file
      echo "# !/bin/bash" > $SH_FILE
      echo "set -eu -o pipefail"  >> $SH_FILE
  
      for URL in $(head -$THREADS $TODO_FILE)
        do
          echo "$URL" >> $COMP_FILE
          echo "$URL &" >> $SH_FILE
        done
      echo "wait" >> $SH_FILE
      # execute it
      bash $SH_FILE
      
      rm $SH_FILE
      cat $TODO_FILE | grep --invert-match --fixed-strings --file=$COMP_FILE > tmp.$PID.$TODO_FILE || true
      mv tmp.$PID.$TODO_FILE $TODO_FILE
    done
  IFS=$OLDIFS
	
# reget the failed downloads

	cat wget.log | cut -d'.' -f5 | grep -Ev "^$" | $SORT > tmp.$PID.wget_ok.txt
	cat ${URL_FILE} | grep -v -F -f tmp.$PID.wget_ok.txt > tmp.$PID.wget_not_ok.txt
	
  OLDIFS=$IFS
  IFS=$'\n'
	
	for URL in $(cat tmp.$PID.wget_not_ok.txt)
	  do
		  eval "$URL"
		done
		
	IFS=$OLDIFS
	
# cleanup

	rm $TODO_FILE $COMP_FILE || true
	rm tmp.$PID.wget_ok.txt tmp.$PID.wget_not_ok.txt || true