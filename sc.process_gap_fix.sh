# !/bin/bash

#
  source sc.env.sh
  
  case $MKT in
    mf|bse|nse)
      :
      ;;
    idx)
      exit 0
      ;;
    *)
      echo "expecting mf, bse, nse or idx, got = " $MKT
      exit -1
  esac

# gap fix logic
# get dates, do cross product using list of stocks and dates, get actual data stk-date list, make reverse sorted gap file
# remove data for stocks with no gaps, produce reverse sorted file
  
  #cut -d',' -f1,2 $INS_FILE | tr -d '"' | $SORT > tmp.${PID}.stocks_in_of_inc.txt
  cut -d',' -f1,2 $OF_INC | tr -d '"' | $SORT -u > tmp.${PID}.stocks_in_of_inc.txt

  cut -d',' -f3 $OF_INC   | tr -d '"' | grep -v ',0,NULL$' | $SORT -u > tmp.${PID}.dates_in_of_inc.txt
  cut -d',' -f3 $IDX_FILE |                                  $SORT -u > tmp.${PID}.idx_dates.txt
  ED=$(date --date="$(cat tmp.${PID}.dates_in_of_inc.txt | grep -Ff tmp.${PID}.idx_dates.txt | tail -1)" +%Y-%m-%d)
  SD=$(date --date="$(cat tmp.${PID}.dates_in_of_inc.txt | grep -Ff tmp.${PID}.idx_dates.txt | head -1)" +%Y-%m-%d)
  if [ $MKT == 'mf' ]
    then
      cat $INS_FILE | grep -i '"Open Ended","Growth"' | cut -d',' -f2 | tr -d '"' > tmp.${PID}.id_open_ended_growth.txt
      #ED=$(date --date="$(echo ${ED} | sed 's;-;;g') - 7 days" +%Y-%m-%d)
    else
      :
  fi
  #SD=$(date --date="$ED - 24 days" +%Y-%m-%d)
  awk -F, -v SD="${SD}" -v ED="${ED}" '{if ($1 >= SD && $1 <= ED) {print $1} }' tmp.${PID}.idx_dates.txt > tmp.${PID}.dates.txt
  
  join -t',' -1 5 -2 5 -o 1.1 1.2 2.1 tmp.${PID}.stocks_in_of_inc.txt tmp.${PID}.dates.txt > tmp.${PID}.stock_date_cross_product.txt
  cut -d',' -f1,2,3 $OF_FUL | grep -Ff tmp.${PID}.dates.txt > tmp.${PID}.stock_date_actual.txt
  
  #diff --changed-group-format='%<' --unchanged-group-format='' \
  #  tmp.${PID}.stock_date_cross_product.txt tmp.${PID}.stock_date_actual.txt | \
  #  tee tmp.${PID}.stock_date_gap.txt | tac > tmp.${PID}.stock_date_gap_rev.txt || true
  
  #split -n l/4 tmp.${PID}.stock_date_actual.txt tmp.${PID}.stock_date_actual.txt.
  split --line-bytes=25MB tmp.${PID}.stock_date_actual.txt tmp.${PID}.stock_date_actual.txt.
  cp tmp.${PID}.stock_date_cross_product.txt tmp.${PID}.stock_date_gap.txt
 
  for FILE in $(ls -x1 tmp.${PID}.stock_date_actual.txt.*)
    do
      grep -xvFf ${FILE} tmp.${PID}.stock_date_gap.txt > tmp.${PID}.stock_date_gap.interim.txt
      mv tmp.${PID}.stock_date_gap.interim.txt tmp.${PID}.stock_date_gap.txt
      rm $FILE
    done
  
  cat tmp.${PID}.stock_date_gap.txt | tac > tmp.${PID}.stock_date_gap_rev.txt
  
  cut -d',' -f1,2 tmp.${PID}.stock_date_gap.txt | $SORT -u | sed -e 's|$|,|g' > tmp.${PID}.gap_stocks.txt
  
  grep -Ff tmp.${PID}.gap_stocks.txt $OF_FUL | tac > tmp.${PID}.stk_ful_rev.txt
  
# primary input = file with gap data, reverse sorted : MKT, stock, date  
# secondary input -> actuals = file with actual data, reversed sorted on stock name and date
  gawk -F',' -v ACTUALS=tmp.${PID}.stk_ful_rev.txt -f awk.gap_fix.awk tmp.${PID}.stock_date_gap_rev.txt \
    | sed 's|,*$||g' | $SORT > tmp.${PID}.stk_gap.txt
  
  rm ./arch/$MKT.processed.*.txt || true
  $SORT -m $OF_FUL tmp.${PID}.stk_gap.txt | tee tmp.$PID.$OF_FUL.sorted | awk -F "," -v MKT=$MKT '{split($3,date,"-"); print > ".\/arch\/"MKT".processed."date[1]".txt"}'
  mv tmp.$PID.$OF_FUL.sorted $OF_FUL

  $SORT -m $OF_INC tmp.${PID}.stk_gap.txt > tmp.$PID.$OF_INC; mv tmp.$PID.$OF_INC $OF_INC
  
  mv tmp.${PID}.stk_gap.txt $MKT.stk_gap.txt
  rm tmp.$PID.* || true
