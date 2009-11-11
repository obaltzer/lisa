files="data/plants_A_1E4.db data/plants_A_2E4.db data/plants_A_3E4.db data/plants_A_4E4.db data/plants_A_5E4.db data/plants_A_6E4.db data/plants_A_7E4.db data/plants_A_8E4.db data/plants_A_9E4.db data/plants_A_1E5.db data/plants_A_2E5.db data/plants_A_3E5.db data/plants_A_4E5.db data/plants_A_5E5.db data/plants_A_6E5.db data/plants_A_7E5.db data/plants_A_8E5.db data/plants_A_9E5.db data/plants_A_1E6.db"
tracks="1 8 16 24 28 32"
runs=1
results=q2.txt
sequence=0
#cat /dev/null > ${results}
cat /dev/null > tmp.$$
for f in ${files}; do
    for t in ${tracks} ; do
        for r in `seq ${runs}` ; do
            echo ${f} > tmp.$$
            /usr/bin/time -f "%e,%U,%S,%P" \
                python query2.py ${t} ${f} >/dev/null 2>>tmp.$$
            
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
done
rm tmp.$$
