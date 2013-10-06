set datafile separator "\t"
set terminal png size 900,400
set title "Write Latency"
set format y "%g %%"
set xlabel "Seconds"
#set xdata time
set timefmt "%s"
set key left top
set grid
plot "cdf32.log" using ($1/1000):2 with lines lw 2 lt 3 linecolor rgb 'red' title 'WTF (32MB Blocks)', \
"cdf.log" using ($1/1000):2 with lines lw 2 lt 3 linecolor rgb 'green' title 'WTF (64MB Blocks)', \
"cdfh.log" using ($1/1000):2 with lines lw 2 lt 3 title 'HDFS (64MB Blocks)'
