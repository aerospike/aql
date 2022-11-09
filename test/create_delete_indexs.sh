#! /usr/bin/env bash

if [ $# -eq 0 ]
  then
    asadm --enable -e 'manage sindex create numeric indint ns test set demo bin binint'
    asadm --enable -e 'manage sindex create string indstring ns test set demo bin binstring'
    asadm --enable -e 'manage sindex create geo2dsphere indgeo ns test set demo bin bingeo'
    asadm --enable -e 'manage sindex create string indmap ns test set demo bin binmap'
    asadm --enable -e 'manage sindex create numeric indmap1 ns test set demo bin bincastmap'
    asadm --enable -e 'manage sindex create numeric indlist ns test set demo bin binlist'
    asadm --enable -e 'manage sindex create numeric numindex ns test set newtest bin b'
    asadm --enable -e 'manage sindex create string strindex ns test set newtest bin c'
else
    asadm --enable -e 'manage sindex delete indint ns test set demo'
    asadm --enable -e 'manage sindex delete indstring ns test set demo'
    asadm --enable -e 'manage sindex delete indgeo ns test set demo'
    asadm --enable -e 'manage sindex delete indmap ns test set demo'
    asadm --enable -e 'manage sindex delete indmap1 ns test set demo'
    asadm --enable -e 'manage sindex delete indlist ns test set demo'
    asadm --enable -e 'manage sindex delete numindex ns test set newtest'
    asadm --enable -e 'manage sindex delete strindex ns test set newtest'
fi
