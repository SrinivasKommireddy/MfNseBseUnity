# !/bin/bash

#
  source sc.env.sh

#
  case $MKT in
    idx)
      for FILE in $(ls -x1 ./in/$MKT.*)
        do
          FILE_CONTENT_CHECK=$(cat $FILE | head -1 \
            | grep "Index Name,Index Date,Open Index Value,High Index Value,Low Index Value,Closing Index Value,Points Change,Change(%),Volume,Turnover (Rs. Cr.),P/E,P/B,Div Yield" \
            | wc -l)
          if [[ $FILE_CONTENT_CHECK != 1 ]]
            then
              echo "$FILE not in expected format. removing"
              rm $FILE
            else
              :
          fi
        done
      cat ./in/$MKT.* |  grep -v 'Index Name,Index Date,Open Index Value' | cut -d',' -f1-6,9 | tr [a-z] [A-Z] | \
        sed 's|,-|,NULL|g' | tr -d ' ' | \
        gawk -v MKT=$MKT -F "," '{ OFS="," }
          {
            STK    = $1
            DATE   = $2; split(DATE,DT,"-")
            OPEN   = $3
            HIGH   = $4
            LOW    = $5
            CLOSE  = $6
            ACLOSE = $6
            VOL    = $7
            print MKT, STK, DT[3] "-" DT[2] "-" DT[1], OPEN, HIGH, LOW, CLOSE, VOL, ACLOSE
          }' > tmp.$PID.$OF_INC
      ;;
    mf)
      cat $INS_FILE | tr -d '"' | cut -d, -f2 | sed -e 's|$|;|g' > tmp.${PID}.inst_list.txt
      for FILE in $(cut -d',' -f2 $AMC_LIST | tr -d '"')
        do
          cat ./in/${MKT}.${FILE}.*.out | \
            grep --fixed-strings --file=tmp.${PID}.inst_list.txt | \
            #parallel -q --pipe --block 10M 
            sed -r -e 's|^([0-9]{6});(.*);(.*);(.*);(.*);(.*)(.$)|\1;\6;NULL;NULL;NULL;\3;NULL;NULL|'  \
              -e 's|(^.*);(..)(-...-)(....);(.*$)|\1;\4\3\2;\5|'              \
              -e 's|(-Jan-)|-01-|' -e 's|(-Feb-)|-02-|'  -e 's|(-Mar-)|-03-|' \
              -e 's|(-Apr-)|-04-|' -e 's|(-May-)|-05-|'  -e 's|(-Jun-)|-06-|' \
              -e 's|(-Jul-)|-07-|' -e 's|(-Aug-)|-08-|'  -e 's|(-Sep-)|-09-|' \
              -e 's|(-Oct-)|-10-|' -e 's|(-Nov-)|-11-|'  -e 's|(-Dec-)|-12-|' \
              -e 's|,||' -e 's|;|,|g' -e "s|^|$MKT,|g"                      | \
            grep --perl-regexp "[a-z]{2,3},[0-9]{6},[0-9-]{10},NULL,NULL,NULL,[1-9]{1}[.0-9]*,NULL,NULL$" >> tmp.$PID.$OF_INC || true
        done
      ;;
    nse)
       #gzip -dS zip ./in/$MKT.*.zip
        for FILE in $(ls -x1 ./in/$MKT.*.zip)
          do
            DT=${FILE:9:9}
            
            UNZIP_NSE_RC=$(unzip -o $FILE -d ./in)
			CM_CSV_FILE=$(echo $UNZIP_NSE_RC | cut -d' ' -f4)
            #CM_ZIP_FILE=$(echo $UNZIP_NSE_RC | cut -d' ' -f4)
            #UNZIP_CMZIP_RC=$(unzip -o $CM_ZIP_FILE -d ./in)
            #CM_CSV_FILE=$(echo $UNZIP_CMZIP_RC | cut -d' ' -f4)
            mv $CM_CSV_FILE ./in/$MKT.$DT.csv
          done
        cat ./in/$MKT.*.csv | grep -v "SYMBOL,SERIES,OPEN" | cut -d',' -f1-7,9,11 | grep -E ",EQ,|,BE," | \
          gawk -v MKT=$MKT -F "," '{
            OFS = ","
             m=split("JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC",d,"|")
             for(o=1;o<=m;o++)
               {
                 date[d[o]]=sprintf("%02d",o)
               }
             }
             {
               STK    = $1
               OPEN   = $3
               HIGH   = $4
               LOW    = $5
               CLOSE  = $6
               ACLOSE = $7
               VOL    = $8
               DATE   = $9; split(DATE,DT,"-")
               
               print MKT, STK, DT[3] "-" date[DT[2]] "-" DT[1], OPEN, HIGH, LOW, CLOSE, VOL, ACLOSE
             }' > tmp.$PID.$OF_INC
        ;;
    bse)
      
      for FILE in $(ls -x1 ./in/$MKT.*.zip)
      do
        if [[ -s $FILE ]] ; then
          FILE_NAME=$(echo $FILE | cut -d'.' -f1,2,3)
          gzip --stdout --decompress --keep $FILE > $FILE_NAME.csv
        else
          :
        fi
      done
      
      cat ./in/$MKT.*.csv | grep -v "SC_CODE,SC_NAME" | cut -d',' -f1,5-9,12,16 | \
        sed -r -e "s/^(.{6},)([0-9]*.[0-9]{2},[0-9]*.[0-9]{2},[0-9]*.[0-9]{2},[0-9]*.[0-9]{2},)([0-9]*.[0-9]{2}),([0-9]*,)(..)(-...-)(..)$/$MKT,\120\7\6\5,\2\4\3/g" \
            -e 's|(-Jan-)|-01-|' -e 's|(-Feb-)|-02-|'  -e 's|(-Mar-)|-03-|' \
            -e 's|(-Apr-)|-04-|' -e 's|(-May-)|-05-|'  -e 's|(-Jun-)|-06-|' \
            -e 's|(-Jul-)|-07-|' -e 's|(-Aug-)|-08-|'  -e 's|(-Sep-)|-09-|' \
            -e 's|(-Oct-)|-10-|' -e 's|(-Nov-)|-11-|'  -e 's|(-Dec-)|-12-|' > tmp.$PID.$OF_INC
      # ff for isin, group and type update
      cat $(ls -x1 ./in/$MKT.*.csv | sort | tail -1) | grep -v "SC_CODE,SC_NAME" | cut -d',' -f1,2,3,4,15 | \
        sed -e 's|^|bse,|g' -e 's/[[:space:]]*,/,/g' -e 's/&*//g' -e 's/^/"/' -e 's/$/"/' -e 's/,/","/g' | \
        gawk -F "," '{OFS=","} {print $1,$2,$3,$6,$4,$5}' \
          | grep -v ",\"E\",|,\"F\",|,\"G\",|,\"GC\",|,\"IF\",|,\"IT\"," \
          > tmp.$PID.$MKT.instrument.bhavcopy.txt
      mv tmp.$PID.$MKT.instrument.bhavcopy.txt bse.instrument.bhavcopy.txt
      
	  # F = mutual fund, G = GOI instrument, E = ETF
      
	  cat bse.instrument.scrip.txt        | grep -Ev '\"F\"|\"G\"|\"E\"' > tmp.$PID.bse.instrument.scrip.txt       
      cat bse.instrument.listofscrips.txt | grep -Ev '\"F\"|\"G\"|\"E\"' > tmp.$PID.bse.instrument.listofscrips.txt
      cat bse.instrument.bhavcopy.txt     | grep -Ev '\"F\"|\"G\"|\"E\"' > tmp.$PID.bse.instrument.bhavcopy.txt    

      mv -f tmp.$PID.bse.instrument.scrip.txt        bse.instrument.scrip.txt       
      mv -f tmp.$PID.bse.instrument.listofscrips.txt bse.instrument.listofscrips.txt	  
      mv -f tmp.$PID.bse.instrument.bhavcopy.txt     bse.instrument.bhavcopy.txt    	  

	  cat bse.instrument.scrip.txt        | cut -d, -f2 | $SORT -u > tmp.$PID.bse.code.scrip.txt
	  cat bse.instrument.listofscrips.txt | cut -d, -f2 | $SORT -u > tmp.$PID.bse.code.listofscrips.txt
	  cat bse.instrument.bhavcopy.txt     | cut -d, -f2 | $SORT -u > tmp.$PID.bse.code.bhavcopy.txt
	  
      cat tmp.$PID.bse.code.scrip.txt tmp.$PID.bse.code.listofscrips.txt tmp.$PID.bse.code.bhavcopy.txt | \
      $SORT -u > tmp.$PID.bse.code_union.txt
      
      join -t, -1 1 -2 2 tmp.$PID.bse.code_union.txt bse.instrument.scrip.txt -o 2.1,2.2,2.3,2.4,2.5,2.6 > tmp.$PID.bse.instrument.scrip.txt
      join -t, -1 1 -2 2 tmp.$PID.bse.code_union.txt bse.instrument.scrip.txt -o 1.1 -v 1 > tmp.$PID.bse.code_after_scrip_join.txt
      
      join -t, -1 1 -2 2 tmp.$PID.bse.code_after_scrip_join.txt bse.instrument.bhavcopy.txt -o 2.1,2.2,2.3,2.4,2.5,2.6 > tmp.$PID.bse.instrument.bhavcopy.txt
      join -t, -1 1 -2 2 tmp.$PID.bse.code_after_scrip_join.txt bse.instrument.bhavcopy.txt -o 1.1 -v 1                > tmp.$PID.bse.code_after_bhavcopy_join.txt
      
      join -t, -1 1 -2 2 tmp.$PID.bse.code_after_bhavcopy_join.txt bse.instrument.listofscrips.txt -o 2.1,2.2,2.3,2.4,2.5,2.6 > tmp.$PID.bse.instrument.listofscrips.txt
      join -t, -1 1 -2 2 tmp.$PID.bse.code_after_bhavcopy_join.txt bse.instrument.listofscrips.txt -o 1.1 -v 1                > tmp.$PID.bse.code_after_listofscrips_join.txt
      
      #join -t, -1 1 -2 2 tmp.$PID.bse.code_union.txt bse.instrument.listofscrips.txt -o 2.1,2.2,2.3,2.4,2.5,2.6 > tmp.$PID.bse.instrument.listofscrips.txt
      #join -t, -1 1 -2 2 tmp.$PID.bse.code_union.txt bse.instrument.listofscrips.txt -o 1.1 -v 1                > tmp.$PID.bse.code_after_listofscrips_join.txt
	  #
      #join -t, -1 1 -2 2 tmp.$PID.bse.code_after_listofscrips_join.txt bse.instrument.scrip.txt -o 2.1,2.2,2.3,2.4,2.5,2.6 > tmp.$PID.bse.instrument.scrip.txt
      #join -t, -1 1 -2 2 tmp.$PID.bse.code_after_listofscrips_join.txt bse.instrument.scrip.txt -o 1.1 -v 1                > tmp.$PID.bse.code_after_scrip_join.txt
      #
      #join -t, -1 1 -2 2 tmp.$PID.bse.code_after_scrip_join.txt bse.instrument.bhavcopy.txt -o 2.1,2.2,2.3,2.4,2.5,2.6 > tmp.$PID.bse.instrument.bhavcopy.txt
      #join -t, -1 1 -2 2 tmp.$PID.bse.code_after_scrip_join.txt bse.instrument.bhavcopy.txt -o 1.1 -v 1                > tmp.$PID.bse.code_after_bhavcopy_join.txt
      

      cat tmp.$PID.bse.instrument.listofscrips.txt tmp.$PID.bse.instrument.scrip.txt tmp.$PID.bse.instrument.bhavcopy.txt > bse.instrument.txt
      
      rm tmp.$PID.bse.instrument.scrip.txt tmp.$PID.bse.instrument.listofscrips.txt tmp.$PID.bse.instrument.bhavcopy.txt
      rm tmp.$PID.bse.code_after_scrip_join.txt tmp.$PID.bse.code_after_bhavcopy_join.txt 
      mv tmp.$PID.bse.code_after_listofscrips_join.txt bse.instrument.code_unmatched.txt

      ;;
  *)
      echo "unexpected value for market, market = " $MKT
      ;;
  esac
  
  #get rid of holiday data
  if [[ ${MKT} = 'mf' ]]
    then
      $SORT -u tmp.$PID.$OF_INC | grep -F "$(cat idx.ful.txt | cut -d',' -f3 | $SORT -u | sed -e "s|^|,|g" -e "s|$|,|g")" \
        > tmp.${PID}.$OF_INC.sorted; mv tmp.${PID}.$OF_INC.sorted $OF_INC; rm tmp.$PID.$OF_INC
    else
      $SORT -u tmp.$PID.$OF_INC > tmp.${PID}.$OF_INC.sorted; mv tmp.${PID}.$OF_INC.sorted $OF_INC; rm tmp.$PID.$OF_INC
  fi
  
  if [[ $(ls -c1 ./arch/$MKT.* | wc -l) -eq 0 ]]
    then
      cat $OF_INC | awk -F "," -v MKT=$MKT '{split($3,date,"-"); print > ".\/arch\/"MKT".raw."date[1]".txt"}'
      for YEAR in $(ls -C1 ./arch/$MKT.raw.*.txt | cut -d'.' -f4)
        do
          # <<< grep -F "$(cat $IDX_FILE | cut -d',' -f3 | $SORT -u)" >>> prevents mf data for sat/sun/holidy from getting through
          cat ./arch/$MKT.raw.$YEAR.txt | grep -F "$(cat $IDX_FILE | cut -d',' -f3 | sed -re 's|(.*)|,\1|g'| $SORT -u)" > ./arch/$MKT.processed.$YEAR.txt
        done
    else
      START_YEAR=$(cat $OF_INC | cut -d',' -f3 | $SORT -u | head -1 | cut -d'-' -f1)
        END_YEAR=$(cat $OF_INC | cut -d',' -f3 | $SORT -u | tail -1 | cut -d'-' -f1)
      #rm tmp.$PID.arch.raw.txt tmp.$PID.arch.processed.txt || true
      for (( c = $START_YEAR; c <= $END_YEAR; c++ ))
        do
          cat ./arch/$MKT.processed.${c}.txt >> tmp.$PID.arch.processed.txt
          cat ./arch/$MKT.raw.${c}.txt       >> tmp.$PID.arch.raw.txt
        done
      #remove revised data in INC file from arch files
      #rm tmp.$PID.exclude_date.txt || true
      cat $OF_INC | cut -d',' -f'1-3' | sed -re 's|(.*)|\1,|g' | $SORT -u > tmp.$PID.exclude_mkt_code_date.txt
      grep --invert-match --fixed-strings --file=tmp.$PID.exclude_mkt_code_date.txt tmp.$PID.arch.processed.txt | $SORT > tmp.$PID.clean_arch.processed.txt || true
      grep --invert-match --fixed-strings --file=tmp.$PID.exclude_mkt_code_date.txt tmp.$PID.arch.raw.txt       | $SORT > tmp.$PID.clean_arch.raw.txt       || true
      $SORT -m $OF_INC tmp.$PID.clean_arch.raw.txt       | awk -F "," -v MKT=$MKT '{split($3,date,"-"); print > ".\/arch\/"MKT".raw."date[1]".txt"}'
      $SORT -m $OF_INC tmp.$PID.clean_arch.processed.txt | awk -F "," -v MKT=$MKT '{split($3,date,"-"); print > ".\/arch\/"MKT".processed."date[1]".txt"}'
      rm tmp.$PID.clean_arch.raw.txt tmp.$PID.clean_arch.processed.txt tmp.$PID.exclude_mkt_code_date.txt \
         tmp.$PID.arch.processed.txt tmp.$PID.arch.raw.txt
  fi
  
  $SORT -m ./arch/${MKT}.processed.*.txt > $OF_FUL
  
  if [ $MKT = 'idx' ]
    then
      # date logic
      
      MINDT=$(date -d "$(cat $OF_FUL | cut -d',' -f3 | $SORT -u | head -1)" +%s)
      MAXDT=$(date -d "$(cat $OF_INC | cut -d',' -f3 | $SORT -u | tail -1)" +%s)
      
      gawk -F, -v SD=$MINDT -v ED=$MAXDT 'BEGIN {for (i=SD;i<=ED;i+=60*60*24) print "," strftime("%Y-%m-%d",i)}' > idx.date.txt
    else
      :
  fi
  
  rm tmp.$PID.* #./in/$MKT.*.zip