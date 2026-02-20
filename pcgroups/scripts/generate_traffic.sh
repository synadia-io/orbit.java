#!/bin/bash
nats bench js pub async foo --multisubject --sleep 10ms --multisubjectmax 100 --stream foo
