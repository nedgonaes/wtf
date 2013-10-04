set datafile separator "\t"
set terminal png size 900,400
set title "Write Throughput"
set ylabel "MB/s"
set xlabel "Seconds"
#set xdata time
set timefmt "%s"
set key left top
set grid
plot "tput.log" using 1:($2*64/5) with lines lw 2 lt 3 title 'WTF (5 Nodes)'
