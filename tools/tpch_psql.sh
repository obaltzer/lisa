#!/bin/bash
# vim: sts=4 et tw=0
SF=$1
region=region_${SF}
nation=nation_${SF}
customer=customer_${SF}
orders=orders_${SF}
lineitem=lineitem_${SF}

create()
{
    psql << EOF 
        DROP TABLE IF EXISTS ${region};
        CREATE TABLE ${region} (
            id INTEGER
        );

        DROP TABLE IF EXISTS ${nation};
        CREATE TABLE ${nation} (
	    id INTEGER,
            region_id INTEGER
        );

        DROP TABLE IF EXISTS ${customer};
        CREATE TABLE ${customer} (
            id INTEGER,
            nation_id INTEGER
        );

        DROP TABLE IF EXISTS ${orders};
        CREATE TABLE ${orders} (
            id INTEGER,
            customer_id INTEGER
        );

        DROP TABLE IF EXISTS ${lineitem};
        CREATE TABLE ${lineitem} (
            order_id INTEGER,
            quantity INTEGER,
            price DOUBLE PRECISION
        );
EOF
}

delete()
{
    psql << EOF 
        DROP TABLE IF EXISTS ${region};
        DROP TABLE IF EXISTS ${nation};
        DROP TABLE IF EXISTS ${customer};
        DROP TABLE IF EXISTS ${orders};
        DROP TABLE IF EXISTS ${lineitem};
EOF
}

load()
{
    # Check for files
    for t in ${region} ${nation} ${customer} ${orders} ${lineitem} ; do
        if [ ! -f ${t}.txt ] ; then
            echo "${t}.txt does not exists."
            return 1
        fi
    done
    
    for t in ${region} ${nation} ${customer} ${orders} ${lineitem} ; do
        echo "Loading ${t}"
        cat ${t}.txt | psql -c "COPY ${t} FROM STDIN WITH DELIMITER '|';"
    done
}

index()
{
    echo "Indexing ${region}"
    psql << EOF
        DROP INDEX IF EXISTS ${region}_id_idx;
        CREATE INDEX ${region}_id_idx ON ${region} (id);
EOF

    echo "Indexing ${nation}"
    psql << EOF
        DROP INDEX IF EXISTS ${nation}_id_idx;
        CREATE INDEX ${nation}_id_idx ON ${nation} (id);
        DROP INDEX IF EXISTS ${nation}_region_id_idx;
        CREATE INDEX ${nation}_region_id_idx ON ${nation} (region_id);
EOF

    echo "Indexing ${customer}"
    psql << EOF
        DROP INDEX IF EXISTS ${customer}_id_idx;
        CREATE INDEX ${customer}_id_idx ON ${customer} (id);
        DROP INDEX IF EXISTS ${customer}_nation_id_idx;
        CREATE INDEX ${customer}_nation_id_idx ON ${customer} (nation_id);
EOF

    echo "Indexing ${orders}"
    psql << EOF
        DROP INDEX IF EXISTS ${orders}_id_idx;
        CREATE INDEX ${orders}_id_idx ON ${orders} (id);
        DROP INDEX IF EXISTS ${orders}_customer_id_idx;
        CREATE INDEX ${orders}_customer_id_idx ON ${orders} (customer_id);
EOF

    echo "Indexing ${lineitem}"
    psql << EOF
        DROP INDEX IF EXISTS ${lineitem}_order_id_idx;
        CREATE INDEX ${lineitem}_order_id_idx ON ${lineitem} (order_id);
EOF
}

all()
{
    create
    load
    index
}

if [ ! "$2" ] ; then
    echo "no command specified. Try create, delete, load, index."
    exit 1
fi

$2
