#!/bin/bash

mapfile -t ports < scis_ports.txt

rank=${SLURM_PROCID:-0}
ntasks=${SLURM_NTASKS:-1}
num_ports=${#ports[@]}

if (( ntasks < num_ports )); then
    ports=("${ports[@]:0:ntasks}")
    num_ports=$ntasks
fi

procs_per_port=$(( ntasks / num_ports ))
index=$(( rank / procs_per_port ))
selected_port=${ports[$index]}

echo "Rank $rank using port $selected_port"

./main --port "$selected_port" "$@"
