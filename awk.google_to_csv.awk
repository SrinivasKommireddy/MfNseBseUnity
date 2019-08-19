BEGIN {
  OFS=","
  CURR_DTTM=""
  PREV_DTTM=""
}
{
  FILE=$1
  split($1,FILE_NAME,"."); STK=FILE_NAME[3]
  #print FILE, FILE_NAME[3]
  
  while ( (getline < FILE ) != 0 ) 
    {
      IN_STRING=$1
  
      if (substr(IN_STRING,1,1) == "a" )
        {
          BASE_DTTM=substr(IN_STRING,2,10)
          CURR_DTTM=BASE_DTTM
        }
      else
        if (substr(IN_STRING,1,1) ~ /[0-9]/ )
          {
            CURR_DTTM=BASE_DTTM+(IN_STRING*60*60*24)
          }
        else
          {
            DUMMY=0
          }
    
      if ( strftime("%Y-%m-%d",CURR_DTTM) == strftime("%Y-%m-%d",PREV_DTTM) )
        {
          DUMMY=0
        }
      else
        {
          CLOSE=$2+0.0 # google data is sometimes bad, closing price is 0, which causes problem in queries
          if (CLOSE==0.0)
            {
              DUMMY=0
            }
          else
            {
              print MKT, STK, strftime("%Y-%m-%d",CURR_DTTM), $5, $3, $4, $2, $6, $2;
            }
        }
      
    #  print PREV_DTTM "," CURR_DTTM
      PREV_DTTM=CURR_DTTM
    }
}