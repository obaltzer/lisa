#!/bin/sh

build_geos()
{
    # Building GEOS
    rm -rf geos-3.2.1
    tar xfj geos-3.2.1.tar.bz2
    cd geos-3.2.1
    CXXFLAGS="-O3 -march=core2" \
    CFLAGS="-O3 -march=core2" \
    ./configure --prefix=$VIRTUAL_ENV
    make -j 2
    make install
    cd ..
}    

build_shapely()
{
    rm -rf Shapely-1.2b6
    tar xfz Shapely-1.2b6.tar.gz
    cd Shapely-1.2b6
    patch -p1 < ../shapely-linux-lib.patch
    cd ..
    easy_install Shapely-1.2b6
}

build_spatialindex()
{
    rm -rf spatialindex-src-1.5.0
    tar xfz spatialindex-src-1.5.0.tar.gz
    cd spatialindex-src-1.5.0
    CXXFLAGS="-O3 -march=core2" \
    CFLAGS="-O3 -march=core2" \
    ./configure --prefix=$VIRTUAL_ENV
    make -j 2
    make install
    cd ..
}

build_rtree()
{
    easy_install rtree
}


if [ "$VIRTUAL_ENV" -a -d "$VIRTUAL_ENV" ] ; then
    build_geos
    build_shapely
    build_spatialindex
    build_rtree
fi
