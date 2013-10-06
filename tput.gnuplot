set datafile separator "\t"
set terminal png size 900,400
set title "Write Throughput"
set ylabel "MB/s"
set xlabel "Seconds"
#set xdata time
set timefmt "%s"
set key left top
set grid
plot "tput32.log" using 1:($2*64/5) with lines lw 2 lt 3 linecolor rgb 'red' title 'WTF (32MB Blocks)' , \
     "tput.log" using 1:($2*64/5) with lines lw 2 lt 3 linecolor rgb 'green' title 'WTF (64MB Blocks)' , \
     "tputh.log" using 1:($2*64/5) with lines lw 2 lt 3 title 'HDFS (64MB Blocks)'
