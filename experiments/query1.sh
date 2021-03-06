#files="data/plants_A_1E4.db data/plants_A_1E5.db data/plants_A_1E6.db data/plants_A_1E7.db"
#tracks="1 2 3 4 5 6 7 8 9 10 11 12"
#runs=10
files="data/plants_A_1E5.db data/plants_A_1E6.db"
#files="data/plants_A_1E3.db data/plants_A_1E4.db"
#tracks="14 16 18"
#tracks="1 2 3 4 5 6 7 8 9 10 11 12 14 16 18"
tracks="14 16 18 20 22 24 26 28 30 32 34"
runs=1
results=q1.txt
sequence=0
# cat /dev/null > ${results}
cat /dev/null > tmp.$$
for f in ${files}; do
    for t in ${tracks} ; do
        for r in `seq ${runs}` ; do
            echo ${f} > tmp.$$
            /usr/bin/time -f "%e,%U,%S,%P" \
                python query1.py ${t} ${f} >/dev/null 2>>tmp.$$
            
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
