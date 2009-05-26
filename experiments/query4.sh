tracks="1 2 3 4 5 6 7 8 9 10 11 12"
runs=5
results=q4.txt
sequence=0
cat /dev/null > ${results}
for t in ${tracks} ; do
    for r in `seq ${runs}` ; do
        cat /dev/null > tmp.$$
        
        /usr/bin/time -f "%e,%U,%S,%P" \
            python query4.py ${t} >/dev/null 2>>tmp.$$
        
        sequence=`expr ${sequence} + 1`
        result="${sequence}"
        result=$(
            cat tmp.$$ | { 
                while read l ; do
                    result="${result},${l}"
                done 
                echo ${result}
            } 
        )
        echo ${result} >> ${results}
        echo ${result}
    done
done
rm tmp.$$
