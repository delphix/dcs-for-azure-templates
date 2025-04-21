#!/bin/bash

for PIPELINE in $(ls -d *_pl)
do
  if test -f $PIPELINE.zip; then
     echo "Processing $PIPELINE"
     unzip -o $PIPELINE.zip
     cat $PIPELINE/$PIPELINE.json | jq '.' > $PIPELINE/$PIPELINE.json.fmt
     mv $PIPELINE/$PIPELINE.json.fmt $PIPELINE/$PIPELINE.json
     cat $PIPELINE/manifest.json | jq '.' > $PIPELINE/manifest.json.fmt
     mv $PIPELINE/manifest.json.fmt $PIPELINE/manifest.json
     rm $PIPELINE.zip
  elif test -f $PIPELINE/$PIPELINE.json; then
     cat $PIPELINE/$PIPELINE.json | jq '.' > $PIPELINE/$PIPELINE.json.fmt
     mv $PIPELINE/$PIPELINE.json.fmt $PIPELINE/$PIPELINE.json
     if test -f $PIPELINE/manifest.json; then
       cat $PIPELINE/manifest.json | jq '.' > $PIPELINE/manifest.json.fmt
       mv $PIPELINE/manifest.json.fmt $PIPELINE/manifest.json
     fi
  fi
done
