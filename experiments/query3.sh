tracks="1 2 4 6 8 10 12 14 16 18 20 22"
runs=1
results=q3.txt
sequence=0
#cat /dev/null > ${results}
for t in ${tracks} ; do
    for r in `seq ${runs}` ; do
        cat /dev/null > tmp.$$
        
        /usr/bin/time -f "%e,%U,%S,%P" \
            python query3.py ${t} >/dev/null 2>>tmp.$$
        
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
